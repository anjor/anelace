package anelace

import (
	"fmt"
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/chunker"
	"github.com/anjor/anelace/internal/chunker/buzhash"
	"github.com/anjor/anelace/internal/chunker/fixedsize"
	"github.com/anjor/anelace/internal/chunker/rabin"
	"github.com/anjor/anelace/internal/collector"
	"github.com/anjor/anelace/internal/collector/fixedcidrefsize"
	"github.com/anjor/anelace/internal/collector/fixedoutdegree"
	"github.com/anjor/anelace/internal/collector/noop"
	"github.com/anjor/anelace/internal/collector/trickle"
	"github.com/anjor/anelace/internal/constants"
	"github.com/anjor/anelace/internal/encoder"
	"github.com/anjor/anelace/internal/encoder/unixfsv1"
	"github.com/anjor/anelace/internal/util/argparser"
	"github.com/anjor/anelace/internal/util/text"
	"github.com/klauspost/cpuid/v2"
	"github.com/pborman/getopt/v2"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/ipfs/go-qringbuf"
)

var availableChunkers = map[string]anlchunker.Initializer{
	"fixed-size": fixedsize.NewChunker,
	"buzhash":    buzhash.NewChunker,
	"rabin":      rabin.NewChunker,
}
var availableCollectors = map[string]anlcollector.Initializer{
	"none":                noop.NewCollector,
	"fixed-cid-refs-size": fixedcidrefsize.NewCollector,
	"fixed-outdegree":     fixedoutdegree.NewCollector,
	"trickle":             trickle.NewCollector,
}
var availableNodeEncoders = map[string]anlencoder.Initializer{
	"unixfsv1": unixfsv1.NewEncoder,
}

type chunkerUnit struct {
	_         constants.Incomparabe
	instance  anlchunker.Chunker
	constants anlchunker.InstanceConstants
}

type carUnit struct {
	_      constants.Incomparabe
	hdr    *anlblock.Header
	region *qringbuf.Region
}

type seenBlocks map[[seenHashSize]byte]uniqueBlockStats
type seenRoots map[[seenHashSize]byte]seenRoot

type Anelace struct {
	// speederization shortcut flags for internal logic
	generateRoots bool

	curStreamOffset  int64
	cfg              config
	statSummary      statSummary
	chunker          chunkerUnit
	collector        anlcollector.Collector
	formattedCid     func(*anlblock.Header) string
	externalEventBus chan<- IngestionEvent
	qrb              *qringbuf.QuantizedRingBuffer
	asyncWG          sync.WaitGroup
	asyncHashingBus  anlblock.AsyncHashingBus
	mu               sync.Mutex
	seenBlocks       seenBlocks
	seenRoots        seenRoots
	carDataQueue     chan carUnit
	carWriteError    chan error
	carDataWriter    io.Writer
}

func NewAnelace() (anl *Anelace) {
	anl = &Anelace{
		cfg: config{
			CidMultibase: "base36",
			HashBits:     256,
			AsyncHashers: 0, // disabling async hashers for now

			StatsActive: statsBlocks,

			// RingBufferSize: 2*constants.HardMaxPayloadSize + 256*1024, // bare-minimum with defaults
			RingBufferSize: 24 * 1024 * 1024, // SANCHECK low seems good somehow... fits in L3 maybe?

			//SANCHECK: these numbers have not been validated
			RingBufferMinRead:  256 * 1024,
			RingBufferSectSize: 64 * 1024,

			emittersStdOut: []string{emRootsJsonl},
			emittersStdErr: []string{emStatsText},

			// not defaults but rather the list of known/configured emitters
			emitters: emissionTargets{
				emNone:        nil,
				emStatsText:   nil,
				emStatsJsonl:  nil,
				emRootsJsonl:  nil,
				emCarV1Stream: nil,
			},

			// some opinionated defaults
			requestedNodeEncoder: "unixfsv1",
			requestedChunker:     "fixed-size_1048576",                                     // 1 MiB static chunking
			requestedCollector:   "trickle_max-direct-leaves=2048_max-sibling-subgroups=8", // trickledag with 4096 MaxDirectLeaves + 8 MaxSiblingSubgroups
			InlineMaxSize:        36,
			hashFunc:             "sha2-256", //sha256 hash
		},
	}

	// init some constants
	{
		s := &anl.statSummary
		s.EventType = "summary"

		s.SysStats.PageSize = os.Getpagesize()
		s.SysStats.Os = runtime.GOOS
		s.SysStats.GoMaxProcs = runtime.GOMAXPROCS(-1)
		s.SysStats.GoVersion = runtime.Version()
		s.SysStats.CPU.NameStr = cpuid.CPU.BrandName
		s.SysStats.CPU.Cores = cpuid.CPU.PhysicalCores
		s.SysStats.CPU.ThreadsPerCore = cpuid.CPU.ThreadsPerCore
		s.SysStats.CPU.FreqMHz = int(cpuid.CPU.Hz / 1000000)
		s.SysStats.CPU.Vendor = cpuid.CPU.VendorString
		s.SysStats.CPU.Family = cpuid.CPU.Family
		s.SysStats.CPU.Model = cpuid.CPU.Model

		feats := cpuid.CPU.FeatureSet()
		sort.Strings(feats)
		s.SysStats.CPU.FeaturesStr = strings.Join(feats, " ")
	}

	return
}

func NewAnelaceFromArgv(argv []string) (anl *Anelace) {

	anl = NewAnelace()

	// populate args
	{
		s := &anl.statSummary
		s.SysStats.ArgvInitial = make([]string, len(argv)-1)
		copy(s.SysStats.ArgvInitial, argv[1:])
	}

	cfg := &anl.cfg
	cfg.initArgvParser()

	// accumulator for multiple errors, to present to the user all at once
	argParseErrs := argparser.Parse(argv, cfg.optSet)

	if cfg.Help || cfg.HelpAll {
		cfg.printUsage()
		os.Exit(0)
	}

	// pre-populate from a compat `ipfs add` command if one was supplied
	if cfg.optSet.IsSet("ipfs-add-compatible-command") {
		if errStrings := cfg.presetFromIPFS(); len(errStrings) > 0 {
			argParseErrs = append(argParseErrs, errStrings...)
		}
	}

	// "invisible" set of defaults (not printed during --help)
	if cfg.requestedCollector == "" && !cfg.optSet.IsSet("collector") {
		cfg.requestedCollector = "none"
		if cfg.requestedNodeEncoder == "" && !cfg.optSet.IsSet("node-encoder") {
			cfg.requestedNodeEncoder = "unixfsv1"
		}
	}

	// has a default
	if cfg.HashBits < 128 || (cfg.HashBits%8) != 0 {
		argParseErrs = append(argParseErrs, "The value of --hash-bits must be a minimum of 128 and be divisible by 8")
	}

	if !inlineMaxSizeWithinBounds(cfg.InlineMaxSize) {
		argParseErrs = append(argParseErrs,
			fmt.Sprintf("--inline-max-size '%s' out of bounds 0 or [4:%d]",
				text.Commify(cfg.InlineMaxSize),
				constants.MaxLeafPayloadSize,
			))
	}

	// Parses/creates the blockmaker/nodeencoder, to pass in turn to the collector chain
	// Not stored in the anl object itself, to cut down on logic leaks
	nodeEnc, errorMessages := anl.setupEncoding()
	argParseErrs = append(argParseErrs, errorMessages...)
	argParseErrs = append(argParseErrs, anl.setupChunker()...)
	argParseErrs = append(argParseErrs, anl.setupCollector(nodeEnc)...)
	argParseErrs = append(argParseErrs, anl.setupEmitters()...)

	// Opts check out - set up the car emitter
	if len(argParseErrs) == 0 && anl.cfg.emitters[emCarV1Stream] != nil {
		argParseErrs = append(argParseErrs, anl.setupCarWriting()...)
	}

	if len(argParseErrs) != 0 {
		fmt.Fprint(argParseErrOut, "\nFatal error parsing arguments:\n\n")
		cfg.printUsage()

		sort.Strings(argParseErrs)
		fmt.Fprintf(
			argParseErrOut,
			"Fatal error parsing arguments:\n\t%s\n",
			strings.Join(argParseErrs, "\n\t"),
		)
		os.Exit(2)
	}

	// Opts *still* check out - take a snapshot of what we ended up with

	// All cid-determining opt come last in a predefined order
	cidOpts := []string{
		"inline-max-size",
		"hash",
		"hash-bits",
		"chunker",
		"collector",
		"node-encoder",
	}
	cidOptsIdx := map[string]struct{}{}
	for _, n := range cidOpts {
		cidOptsIdx[n] = struct{}{}
	}

	// first do the generic options
	cfg.optSet.VisitAll(func(o getopt.Option) {
		switch o.LongName() {
		case "help", "help-all", "ipfs-add-compatible-command":
			// do nothing for these
		default:
			// skip these keys too, they come next
			if _, exists := cidOptsIdx[o.LongName()]; !exists {
				anl.statSummary.SysStats.ArgvExpanded = append(
					anl.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
						o.LongName(),
						o.Value().String(),
					),
				)
			}
		}
	})
	sort.Strings(anl.statSummary.SysStats.ArgvExpanded)

	// now do the remaining cid-determining options
	for _, n := range cidOpts {
		anl.statSummary.SysStats.ArgvExpanded = append(
			anl.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
				n,
				cfg.optSet.GetValue(n),
			),
		)
	}

	return
}

func (anl *Anelace) Destroy() {
	anl.mu.Lock()
	if anl.asyncHashingBus != nil {
		close(anl.asyncHashingBus)
		anl.asyncHashingBus = nil
	}
	anl.qrb = nil
	anl.mu.Unlock()
}

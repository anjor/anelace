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
	"github.com/pborman/getopt/v2"
	"io"
	"os"
	"sort"
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

func NewAnelace() *Anelace {
	return &Anelace{
		cfg:         defaultConfig(),
		statSummary: setStatSummary(),
	}
}

func NewAnelaceFromArgv(argv []string) (anl *Anelace) {

	anl = NewAnelace()
	anl.statSummary.SysStats.ArgvInitial = getInitialArgs(argv)

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

	// has a default
	if cfg.HashBits < 128 || (cfg.HashBits%8) != 0 {
		argParseErrs = append(argParseErrs, fmt.Errorf("The value of --hash-bits must be a minimum of 128 and be divisible by 8"))
	}

	if !inlineMaxSizeWithinBounds(cfg.InlineMaxSize) {
		argParseErrs = append(argParseErrs,
			fmt.Errorf("--inline-max-size '%s' out of bounds 0 or [4:%d]",
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

	logArgParseErrors(argParseErrs, cfg)

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

func (anl *Anelace) SetCarWriter(w io.Writer) {
	anl.carDataWriter = w
}

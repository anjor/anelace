package anelace

import (
	"github.com/klauspost/cpuid/v2"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"

	anlblock "github.com/anjor/anelace/internal/anelace/block"
	"github.com/anjor/anelace/internal/constants"
	"github.com/ipfs/go-qringbuf"

	anlchunker "github.com/anjor/anelace/internal/anelace/chunker"
	"github.com/anjor/anelace/internal/anelace/chunker/buzhash"
	"github.com/anjor/anelace/internal/anelace/chunker/fixedsize"
	"github.com/anjor/anelace/internal/anelace/chunker/rabin"

	anlcollector "github.com/anjor/anelace/internal/anelace/collector"
	"github.com/anjor/anelace/internal/anelace/collector/fixedcidrefsize"
	"github.com/anjor/anelace/internal/anelace/collector/fixedoutdegree"
	"github.com/anjor/anelace/internal/anelace/collector/noop"
	"github.com/anjor/anelace/internal/anelace/collector/trickle"

	anlencoder "github.com/anjor/anelace/internal/anelace/encoder"
	"github.com/anjor/anelace/internal/anelace/encoder/unixfsv1"
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

		s.SysStats.ArgvInitial = make([]string, len(argv)-1)
		copy(s.SysStats.ArgvInitial, argv[1:])

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

func (anl *Anelace) Destroy() {
	anl.mu.Lock()
	if anl.asyncHashingBus != nil {
		close(anl.asyncHashingBus)
		anl.asyncHashingBus = nil
	}
	anl.qrb = nil
	anl.mu.Unlock()
}

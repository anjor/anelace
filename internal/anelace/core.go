package anelace

import (
	"io"
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

func (anl *Anelace) Destroy() {
	anl.mu.Lock()
	if anl.asyncHashingBus != nil {
		close(anl.asyncHashingBus)
		anl.asyncHashingBus = nil
	}
	anl.qrb = nil
	anl.mu.Unlock()
}

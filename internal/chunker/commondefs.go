package anlchunker

import (
	"github.com/anjor/anelace/internal/constants"
)

type InstanceConstants struct {
	_            constants.Incomparabe
	MinChunkSize int
	MaxChunkSize int
}

type Initializer func(
	chunkerCLISubArgs []string,
) (
	instance Chunker,
	constants InstanceConstants,
	initErrorStrings []error,
)

type Chunker interface {
	Split(
		rawDataBuffer []byte,
		useEntireBuffer bool,
		resultCallback SplitResultCallback,
	) error
}

type SplitResultCallback func(
	singleChunkingResult Chunk,
) error

type Chunk struct {
	Size int
}

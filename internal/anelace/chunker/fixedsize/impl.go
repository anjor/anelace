package fixedsize

import (
	anlchunker "github.com/anjor/anelace/internal/anelace/chunker"
)

type fixedSizeChunker struct {
	size int
}

func (c *fixedSizeChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb anlchunker.SplitResultCallback,
) (err error) {

	curIdx := c.size

	for curIdx < len(buf) {
		err = cb(anlchunker.Chunk{Size: c.size})
		if err != nil {
			return
		}
		curIdx += c.size
	}

	if curIdx-c.size < len(buf) && useEntireBuffer {
		err = cb(anlchunker.Chunk{Size: len(buf) - (curIdx - c.size)})
	}
	return
}

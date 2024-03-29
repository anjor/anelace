package fixedsize

import (
	"fmt"
	"github.com/anjor/anelace/internal/chunker"
	"github.com/anjor/anelace/internal/constants"
	"github.com/anjor/anelace/internal/util/argparser"
	"github.com/anjor/anelace/internal/util/text"
	"strconv"
)

func NewChunker(
	args []string,
) (
	_ anlchunker.Chunker,
	_ anlchunker.InstanceConstants,
	initErrs []error,
) {

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = argparser.SubHelp(
			"Splits buffer into equally sized chunks. Requires a single parameter: the\n"+
				"size of each chunk in bytes (IPFS default: 262144)\n",
			nil,
		)
		return
	}

	c := fixedSizeChunker{}

	if len(args) != 2 {
		initErrs = append(initErrs, fmt.Errorf("chunker requires an integer argument, the size of each chunk in bytes"))
	} else {
		sizearg, err := strconv.ParseUint(
			args[1][2:], // stripping off '--'
			10,
			25, // 25bits == 32 * 1024 * 1024 == 32MiB
		)
		if err != nil {
			initErrs = append(initErrs, fmt.Errorf("argument parse failed: %s", err))
		} else {
			c.size = int(sizearg)
		}
	}

	if c.size > constants.MaxLeafPayloadSize {
		initErrs = append(initErrs, fmt.Errorf(
			"provided chunk size '%s' exceeds specified maximum payload size '%s",
			text.Commify(c.size),
			text.Commify(constants.MaxLeafPayloadSize),
		))
	}

	return &c, anlchunker.InstanceConstants{
		MinChunkSize: c.size,
		MaxChunkSize: c.size,
	}, initErrs
}

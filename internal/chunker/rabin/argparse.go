package rabin

import (
	"fmt"
	"github.com/anjor/anelace/internal/chunker"
	"github.com/anjor/anelace/internal/chunker/rabin/bootstrap"
	"github.com/anjor/anelace/internal/util/argparser"

	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewChunker(
	args []string,
) (
	_ anlchunker.Chunker,
	_ anlchunker.InstanceConstants,
	initErrs []error,
) {

	c := rabinChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		initErrs = []error{fmt.Errorf("option set registration failed: %s", err)}
		return
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = argparser.SubHelp(
			"Chunker based on the venerable 'Rabin Fingerprint', similar to the one\n"+
				"used by `restic`, the LBFS, and others. The provided implementation is a\n"+
				"significantly slimmed-down adaptation of multiple \"classic\" versions.",
			optSet,
		)
		return
	}

	// bail early if getopt fails
	if initErrs = argparser.Parse(args, optSet); len(initErrs) > 0 {
		return
	}

	if c.MinSize >= c.MaxSize {
		initErrs = append(initErrs,
			fmt.Errorf("value for 'max-size' must be larger than 'min-size'"),
		)
	}

	var err error
	c.outTable, c.modTable, err = bootstrap.GenerateLookupTables(c.Polynomial, c.WindowSize)
	if err != nil {
		initErrs = append(initErrs, err)
	}

	c.mask = 1<<uint(c.MaskBits) - 1

	if len(initErrs) > 0 {
		return
	}

	// Due to outTable[0] always being 0, this is simply the value 1
	// but derive it longform nevertheless
	c.initState = ((c.outTable[0] << 8) | 1) ^ (c.modTable[c.outTable[0]>>bootstrap.DegShift])
	c.minSansPreheat = c.MinSize - c.WindowSize

	return &c, anlchunker.InstanceConstants{
		MinChunkSize: c.MinSize,
		MaxChunkSize: c.MaxSize,
	}, initErrs
}

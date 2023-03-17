package noop

import (
	"fmt"
	"github.com/anjor/anelace/internal/collector"
	"github.com/anjor/anelace/internal/util/argparser"
)

func NewCollector(args []string, cfg *anlcollector.AnlConfig) (_ anlcollector.Collector, initErrs []error) {

	if args == nil {
		initErrs = argparser.SubHelp(
			"Does not form a DAG, nor emits a root CID. Simply redirects chunked data\n"+
				"to /dev/null. Takes no arguments.\n",
			nil,
		)
		return
	}

	if len(args) > 1 {
		initErrs = append(initErrs, fmt.Errorf("collector takes no arguments"))
	}

	return &nulCollector{cfg}, initErrs
}

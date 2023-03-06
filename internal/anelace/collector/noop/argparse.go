package noop

import (
	anlcollector "github.com/anjor/anelace/internal/anelace/collector"

	"github.com/anjor/anelace/internal/anelace/util/argparser"
)

func NewCollector(args []string, cfg *anlcollector.AnlConfig) (_ anlcollector.Collector, initErrs []string) {

	if args == nil {
		initErrs = argparser.SubHelp(
			"Does not form a DAG, nor emits a root CID. Simply redirects chunked data\n"+
				"to /dev/null. Takes no arguments.\n",
			nil,
		)
		return
	}

	if len(args) > 1 {
		initErrs = append(initErrs, "collector takes no arguments")
	}

	return &nulCollector{cfg}, initErrs
}

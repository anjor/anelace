package fixedcidrefsize

import (
	"fmt"
	"github.com/anjor/anelace/internal/collector"
	"github.com/anjor/anelace/internal/util/argparser"

	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewCollector(args []string, anlCfg *anlcollector.AnlConfig) (_ anlcollector.Collector, initErrs []error) {

	co := &collector{
		AnlConfig: anlCfg,
		state:     state{stack: []*layer{{}}},
	}

	optSet := getopt.New()
	if err := options.RegisterSet("", &co.config, optSet); err != nil {
		initErrs = []error{fmt.Errorf("option set registration failed: %s", err)}
		return
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = argparser.SubHelp(
			"Forms a DAG where the amount of bytes taken by CID references is limited\n"+
				"for every individual node. The IPLD-link referencing overhead, aside from\n"+
				"the CID length itself, is *NOT* considered.",
			optSet,
		)
		return
	}

	// bail early if getopt fails
	if initErrs = argparser.Parse(args, optSet); len(initErrs) > 0 {
		return
	}

	return co, initErrs
}

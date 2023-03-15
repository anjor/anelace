package fixedoutdegree

import (
	"fmt"
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/collector"
	"github.com/anjor/anelace/internal/util/argparser"

	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewCollector(args []string, cfg *anlcollector.AnlConfig) (_ anlcollector.Collector, initErrs []string) {

	co := &collector{
		AnlConfig: cfg,
		state:     state{stack: [][]*anlblock.Header{{}}},
	}

	optSet := getopt.New()
	if err := options.RegisterSet("", &co.config, optSet); err != nil {
		initErrs = []string{fmt.Sprintf("option set registration failed: %s", err)}
		return
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = argparser.SubHelp(
			"Forms a DAG where every node has a fixed outdegree (amount of children).\n"+
				"The last (right-most) node in each DAG layer may have a lower outdegree.",
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

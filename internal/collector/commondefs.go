package anlcollector

import (
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/encoder"
)

type Collector interface {
	AppendData(formLeafBlockAndAppend anlblock.DataSource) (resultingLeafBlock *anlblock.Header)
	AppendBlock(blockToAppendToStream *anlblock.Header)
	FlushState() (rootBlockAfterReducingAndDestroyingObjectState *anlblock.Header)
}

type Initializer func(
	collectorCLISubArgs []string,
	acfg *AnlConfig,
) (instance Collector, initErrorStrings []error)

type AnlConfig struct {
	NodeEncoder anlencoder.NodeEncoder
}

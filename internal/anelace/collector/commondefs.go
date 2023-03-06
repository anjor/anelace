package anlcollector

import (
	anlblock "github.com/anjor/anelace/internal/anelace/block"
	anlencoder "github.com/anjor/anelace/internal/anelace/encoder"
)

type Collector interface {
	AppendData(formLeafBlockAndAppend anlblock.DataSource) (resultingLeafBlock *anlblock.Header)
	AppendBlock(blockToAppendToStream *anlblock.Header)
	FlushState() (rootBlockAfterReducingAndDestroyingObjectState *anlblock.Header)
}

type Initializer func(
	collectorCLISubArgs []string,
	acfg *AnlConfig,
) (instance Collector, initErrorStrings []string)

type AnlConfig struct {
	NodeEncoder anlencoder.NodeEncoder
}

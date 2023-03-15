package anlencoder

import (
	"github.com/anjor/anelace/internal/block"
)

type NodeEncoder interface {
	NewLeaf(leafSource anlblock.DataSource) (leafBlock *anlblock.Header)
	NewLink(blocksToLink []*anlblock.Header) (linkBlock *anlblock.Header)
}

type Initializer func(
	encoderCLISubArgs []string,
	acfg *AnlConfig,
) (instance NodeEncoder, initErrorStrings []string)

type AnlConfig struct {
	HasherBits           int
	HasherName           string
	BlockMaker           anlblock.Maker
	NewLinkBlockCallback func(block *anlblock.Header)
}

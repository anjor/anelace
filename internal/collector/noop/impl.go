package noop

import (
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/collector"
)

type nulCollector struct{ *anlcollector.AnlConfig }

func (*nulCollector) AppendBlock(*anlblock.Header) {}
func (*nulCollector) FlushState() *anlblock.Header { return nil }
func (nc *nulCollector) AppendData(ds anlblock.DataSource) *anlblock.Header {
	return nc.NodeEncoder.NewLeaf(ds)
}

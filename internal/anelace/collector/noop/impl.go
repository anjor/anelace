package noop

import (
	anlblock "github.com/anjor/anelace/internal/anelace/block"
	anlcollector "github.com/anjor/anelace/internal/anelace/collector"
)

type nulCollector struct{ *anlcollector.AnlConfig }

func (*nulCollector) AppendBlock(*anlblock.Header) {}
func (*nulCollector) FlushState() *anlblock.Header { return nil }
func (nc *nulCollector) AppendData(ds anlblock.DataSource) *anlblock.Header {
	return nc.NodeEncoder.NewLeaf(ds)
}

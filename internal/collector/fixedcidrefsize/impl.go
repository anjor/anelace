package fixedcidrefsize

import (
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/collector"
)

type config struct {
	// SANCHECK 160 is based on allowing 2 512-byte CIDs to be added + *very* generous padding
	// Likely need to just turn this into a runtime check instead and be done with it...
	MaxCidRefSize int `getopt:"--max-cid-refs-size=[160:]  Maximum cumulative byte-size of CID references within a node"`
}
type state struct {
	stack []*layer
}
type layer struct {
	cidRefsSize int
	nodes       []*anlblock.Header
}
type collector struct {
	config
	*anlcollector.AnlConfig
	state
}

func (co *collector) FlushState() *anlblock.Header {
	if len(co.stack[len(co.stack)-1].nodes) == 0 {
		return nil
	}

	// it is critical to reset the collector state when we are done - we reuse the object!
	defer func() { co.state = state{stack: []*layer{{}}} }()

	co.compactLayers(true) // merge everything
	return co.stack[len(co.stack)-1].nodes[0]
}

func (co *collector) AppendData(ds anlblock.DataSource) (hdr *anlblock.Header) {
	hdr = co.NodeEncoder.NewLeaf(ds)
	co.AppendBlock(hdr)
	return
}

func (co *collector) AppendBlock(hdr *anlblock.Header) {

	co.stack[0].cidRefsSize += len(hdr.Cid())
	co.stack[0].nodes = append(co.stack[0].nodes, hdr)

	// Compact every time we reach enough nodes on the entry layer
	// Helps relieve memory pressure/consumption on very large DAGs
	if co.stack[0].cidRefsSize >= co.MaxCidRefSize {
		co.compactLayers(false) // do not proceed beyond already-full nodes
	}
}

func (co *collector) compactLayers(fullMergeRequested bool) {

	for stackLayerIdx := 0; stackLayerIdx < len(co.stack); stackLayerIdx++ {
		curLayer := co.stack[stackLayerIdx] // shortcut

		if len(curLayer.nodes) == 1 && len(co.stack)-1 == stackLayerIdx ||
			!fullMergeRequested && curLayer.cidRefsSize < co.MaxCidRefSize {
			break
		}

		// we got work to do - instantiate next stack if needed
		if len(co.stack)-1 == stackLayerIdx {
			co.stack = append(co.stack, &layer{})
		}

		var curIdx, lastCutIdx, runningRefSize int
		for curLayer.cidRefsSize > co.MaxCidRefSize ||
			fullMergeRequested && lastCutIdx < len(curLayer.nodes) {

			for curIdx < len(curLayer.nodes) {
				if runningRefSize+len(curLayer.nodes[curIdx].Cid()) > co.MaxCidRefSize {
					break
				}
				runningRefSize += len(curLayer.nodes[curIdx].Cid())
				curIdx++
			}

			linkHdr := co.NodeEncoder.NewLink(
				curLayer.nodes[lastCutIdx:curIdx],
			)
			co.stack[stackLayerIdx+1].nodes = append(co.stack[stackLayerIdx+1].nodes, linkHdr)
			co.stack[stackLayerIdx+1].cidRefsSize += len(linkHdr.Cid())

			curLayer.cidRefsSize -= runningRefSize
			runningRefSize = 0
			lastCutIdx = curIdx
		}

		// shift everything to the last cut, without realloc
		curLayer.nodes = curLayer.nodes[:copy(
			curLayer.nodes,
			curLayer.nodes[lastCutIdx:],
		)]
	}
}

package anelace

import (
	"encoding/json"
	"fmt"
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/util/text"
	"log"
	"strings"

	"github.com/ipfs/go-qringbuf"
)

// The bit reduction is to make the internal seen map smaller memory-wise
// That many bits are taken off the *end* of any non-identity CID
// We could remove the shortening, but for now there's no reason to, and
// as an extra benefit it makes the murmur3 case *way* easier to code
const seenHashSize = 128 / 8

type seenRoot struct {
	order int
	cid   []byte
}

func seenKey(b *anlblock.Header) (id *[seenHashSize]byte) {
	if b == nil ||
		b.IsCidInlined() ||
		b.DummyHashed() {
		return
	}

	cid := b.Cid()
	id = new([seenHashSize]byte)
	copy(
		id[:],
		cid[(len(cid)-seenHashSize):],
	)
	return
}

type cpu struct {
	NameStr        string `json:"name"`
	FeaturesStr    string `json:"features"`
	Cores          int    `json:"cores"`
	ThreadsPerCore int    `json:"threadsPerCore"`
	FreqMHz        int    `json:"mhz"`
	Vendor         string `json:"vendor"`
	Family         int    `json:"family"`
	Model          int    `json:"model"`
}

type sysStats struct {
	qringbuf.Stats
	ElapsedNsecs int64 `json:"elapsedNanoseconds"`

	// getrusage() section
	CpuUserNsecs int64 `json:"cpuUserNanoseconds"`
	CpuSysNsecs  int64 `json:"cpuSystemNanoseconds"`
	MaxRssBytes  int64 `json:"maxMemoryUsed"`
	MinFlt       int64 `json:"cacheMinorFaults"`
	MajFlt       int64 `json:"cacheMajorFaults"`
	BioRead      int64 `json:"blockIoReads,omitempty"`
	BioWrite     int64 `json:"blockIoWrites,omitempty"`
	Sigs         int64 `json:"signalsReceived,omitempty"`
	CtxSwYield   int64 `json:"contextSwitchYields"`
	CtxSwForced  int64 `json:"contextSwitchForced"`

	// for context
	PageSize   int    `json:"pageSize"`
	CPU        cpu    `json:"cpu"`
	GoMaxProcs int    `json:"goMaxProcs"`
	Os         string `json:"os"`

	ArgvExpanded []string `json:"argvExpanded"`
	ArgvInitial  []string `json:"argvInitial"`
	GoVersion    string   `json:"goVersion"`
}

type statSummary struct {
	EventType string `json:"event"`
	Dag       struct {
		Nodes   int64 `json:"nodes"`
		Size    int64 `json:"wireSize"`
		Payload int64 `json:"payload"`
	} `json:"logicalDag"`
	Streams  int64       `json:"subStreams"`
	Roots    []rootStats `json:"roots,omitempty"`
	SysStats sysStats    `json:"sys"`
}
type rootStats struct {
	Cid         string `json:"cid"`
	SizeDag     uint64 `json:"wireSize"`
	SizePayload uint64 `json:"payload"`
	Dup         bool   `json:"duplicate,omitempty"`
}

type uniqueBlockStats struct {
	sizeBlock int
	isData    bool
}

func (anl *Anelace) OutputSummary() {

	// no stats emitters - nowhere to output
	if anl.cfg.emitters[emStatsText] == nil && anl.cfg.emitters[emStatsJsonl] == nil {
		return
	}

	smr := &anl.statSummary
	var totalUCount, totalUWeight, leafUWeight, leafUCount int64

	if anl.seenBlocks != nil && len(anl.seenBlocks) > 0 {
		for _, b := range anl.seenBlocks {
			totalUCount++
			totalUWeight += int64(b.sizeBlock)
			if b.isData {
				leafUCount++
				leafUWeight += int64(b.sizeBlock)
			}
		}
	}

	if statsJsonlOut := anl.cfg.emitters[emStatsJsonl]; statsJsonlOut != nil {
		// emit the JSON last, so that piping to e.g. `jq` works nicer
		defer func() {

			if smr.Roots == nil {
				smr.Roots = []rootStats{}
			}
			jsonl, err := json.Marshal(smr)
			if err != nil {
				log.Fatalf("Encoding '%s' failed: %s", emStatsJsonl, err)
			}

			if _, err := fmt.Fprintf(statsJsonlOut, "%s\n", jsonl); err != nil {
				log.Fatalf("Emitting '%s' failed: %s", emStatsJsonl, err)
			}
		}()
	}

	statsTextOut := anl.cfg.emitters[emStatsText]
	if statsTextOut == nil {
		return
	}

	var substreamsDesc string
	if anl.cfg.MultipartStream {
		substreamsDesc = fmt.Sprintf(
			" from %s substreams",
			text.Commify64(anl.statSummary.Streams),
		)
	}

	writeTextOutf := func(f string, args ...interface{}) {
		if _, err := fmt.Fprintf(statsTextOut, f, args...); err != nil {
			log.Fatalf("Emitting '%s' failed: %s", emStatsText, err)
		}
	}

	writeTextOutf(
		"\nRan on %d-core/%d-thread %s"+
			"\nProcessing took %0.2f seconds using %0.2f vCPU and %0.2f MiB peak memory"+
			"\nPerforming %s system reads using %0.2f vCPU at about %0.2f MiB/s"+
			"\nIngesting payload of:%17s bytes%s\n\n",

		smr.SysStats.CPU.Cores,
		smr.SysStats.CPU.Cores*smr.SysStats.CPU.ThreadsPerCore,
		smr.SysStats.CPU.NameStr,

		float64(smr.SysStats.ElapsedNsecs)/
			1000000000,

		float64(smr.SysStats.CpuUserNsecs)/
			float64(smr.SysStats.ElapsedNsecs),

		float64(smr.SysStats.MaxRssBytes)/
			(1024*1024),

		text.Commify64(smr.SysStats.ReadCalls),

		float64(smr.SysStats.CpuSysNsecs)/
			float64(smr.SysStats.ElapsedNsecs),

		(float64(smr.Dag.Payload)/(1024*1024))/
			(float64(smr.SysStats.ElapsedNsecs)/1000000000),

		text.Commify64(smr.Dag.Payload),

		substreamsDesc,
	)

	if smr.Dag.Nodes > 0 {
		writeTextOutf(
			"Forming DAG covering:%17s bytes of %s logical nodes\n",
			text.Commify64(smr.Dag.Size), text.Commify64(smr.Dag.Nodes),
		)
	}

	descParts := make([]string, 0, 32)

	descParts = append(descParts, fmt.Sprintf(
		"\nDataset deduped into:%17s bytes over %s unique leaf nodes\n",
		text.Commify64(leafUWeight), text.Commify64(leafUCount),
	))

	if anl.cfg.requestedCollector != "none" {
		descParts = append(descParts, fmt.Sprintf(
			"Linked as streams by:%17s bytes over %s unique DAG-PB nodes\n"+
				"Taking a grand-total:%17s bytes, ",
			text.Commify64(totalUWeight-leafUWeight), text.Commify64(totalUCount-leafUCount),
			text.Commify64(totalUWeight),
		))
	} else {
		descParts = append(descParts, fmt.Sprintf("%44s", ""))
	}

	descParts = append(descParts, fmt.Sprintf(
		"%.02f%% of original, %.01fx smaller\n",
		100*float64(totalUWeight)/float64(smr.Dag.Payload),
		float64(smr.Dag.Payload)/float64(totalUWeight),
	))

	writeTextOutf("%s\n", strings.Join(descParts, ""))
}

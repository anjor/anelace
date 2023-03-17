package anelace

import (
	"github.com/anjor/anelace/internal/constants"
	"github.com/klauspost/cpuid/v2"
	"github.com/pborman/getopt/v2"
	"os"
	"runtime"
	"sort"
	"strings"
)

type config struct {
	optSet *getopt.Set

	// where to output
	emitters emissionTargets

	//
	// Bulk of CLI options definition starts here, the rest further down in initArgvParser()
	//

	Help            bool `getopt:"-h --help         Display basic help"`
	HelpAll         bool `getopt:"--help-all        Display full help including options for every currently supported chunker/collector/encoder"`
	MultipartStream bool `getopt:"--multipart       Expect multiple SInt64BE-size-prefixed streams on stdIN"`
	SkipNulInputs   bool `getopt:"--skip-nul-inputs Instead of emitting an IPFS-compatible zero-length CID, skip zero-length streams outright"`

	emittersStdErr []string // Emitter spec: option/helptext in initArgvParser()
	emittersStdOut []string // Emitter spec: option/helptext in initArgvParser()

	// no-option-attached, these are instantiation error accumulators
	erroredChunkers     []string
	erroredCollectors   []string
	erroredNodeEncoders []string

	// Recommendation in help based on largest identity CID that fits in 63 chars (dns limit)
	// of multibase-id prefixed encoding: 1 + ceil( (4+36) * log(256) / log(36) )
	// The base36 => 36bytes match is a coincidence: for base 32 the max value is 34 bytes
	InlineMaxSize      int `getopt:"--inline-max-size=bytes         Use identity-CID to refer to blocks having on-wire size at or below the specified value (36 is recommended), 0 disables"`
	AsyncHashers       int `getopt:"--async-hashers=integer         Number of concurrent short-lived goroutines performing hashing. Set to 0 (disable) for predictable benchmarking. Default:"`
	RingBufferSize     int `getopt:"--ring-buffer-size=bytes        The size of the quantized ring buffer used for ingestion. Default:"`
	RingBufferSectSize int `getopt:"--ring-buffer-sync-size=bytes   (EXPERT SETTING) The size of each buffer synchronization sector. Default:"` // option vaguely named 'sync' to not confuse users
	RingBufferMinRead  int `getopt:"--ring-buffer-min-sysread=bytes (EXPERT SETTING) Perform next read(2) only when the specified amount of free space is available in the buffer. Default:"`

	StatsActive uint `getopt:"--stats-active=uint   A bitfield representing activated stat aggregations: bit0:BlockSizing, bit1:RingbufferTiming. Default:"`

	HashBits     int    `getopt:"--hash-bits=integer    Amount of bits taken from *start* of the hash output. Default:"`
	CidMultibase string `getopt:"--cid-multibase=string Use this multibase when encoding CIDs for output. One of 'base32', 'base36'. Default:"`
	hashFunc     string // hash function to use: option/helptext in initArgvParser()

	requestedChunker     string // Chunker: option/helptext in initArgvParser()
	requestedCollector   string // Collector: option/helptext in initArgvParser()
	requestedNodeEncoder string // The global (for now) node=>block encoder: option/helptext in initArgvParser

	IpfsCompatCmd string `getopt:"--ipfs-add-compatible-command=cmdstring A complete go-ipfs/js-ipfs add command serving as a basis config (any conflicting option will take precedence)"`
}

func defaultConfig() config {
	return config{
		CidMultibase: "base36",
		HashBits:     256,
		AsyncHashers: 0, // disabling async hashers for now

		StatsActive: statsBlocks,

		// RingBufferSize: 2*constants.HardMaxPayloadSize + 256*1024, // bare-minimum with defaults
		RingBufferSize: 24 * 1024 * 1024, // SANCHECK low seems good somehow... fits in L3 maybe?

		//SANCHECK: these numbers have not been validated
		RingBufferMinRead:  256 * 1024,
		RingBufferSectSize: 64 * 1024,

		emittersStdOut: []string{emRootsJsonl},
		emittersStdErr: []string{emStatsText},

		// not defaults but rather the list of known/configured emitters
		emitters: emissionTargets{
			emNone:        nil,
			emStatsText:   nil,
			emStatsJsonl:  nil,
			emRootsJsonl:  nil,
			emCarV1Stream: nil,
		},

		// some opinionated defaults
		requestedNodeEncoder: "unixfsv1",
		requestedChunker:     "fixed-size_1048576",                                     // 1 MiB static chunking
		requestedCollector:   "trickle_max-direct-leaves=2048_max-sibling-subgroups=8", // trickledag with 4096 MaxDirectLeaves + 8 MaxSiblingSubgroups
		InlineMaxSize:        36,
		hashFunc:             "sha2-256", //sha256 hash
	}
}

func setStatSummary() statSummary {
	return statSummary{
		EventType: "summary",
		SysStats: sysStats{
			PageSize:   os.Getpagesize(),
			Os:         runtime.GOOS,
			GoMaxProcs: runtime.GOMAXPROCS(-1),
			GoVersion:  runtime.Version(),
			CPU:        getCpu(),
		},
	}
}

func getCpu() cpu {
	return cpu{
		NameStr:        cpuid.CPU.BrandName,
		Cores:          cpuid.CPU.PhysicalCores,
		ThreadsPerCore: cpuid.CPU.ThreadsPerCore,
		FreqMHz:        int(cpuid.CPU.Hz / 1000000),
		Vendor:         cpuid.CPU.VendorString,
		Family:         cpuid.CPU.Family,
		Model:          cpuid.CPU.Model,
		FeaturesStr:    getFeatureSet(),
	}
}

func getFeatureSet() string {
	feats := cpuid.CPU.FeatureSet()
	sort.Strings(feats)
	return strings.Join(feats, " ")
}

func checkHashBits(hashBits int) error {
	return nil
}

func inlineMaxSizeWithinBounds(ims int) bool {
	return ims == 0 || (ims >= 4 && ims < constants.MaxLeafPayloadSize)
}

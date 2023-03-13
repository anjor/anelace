package anelace

import (
	"encoding/base32"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/multiformats/go-base36"

	anlblock "github.com/anjor/anelace/internal/anelace/block"
	anlcollector "github.com/anjor/anelace/internal/anelace/collector"
	anlencoder "github.com/anjor/anelace/internal/anelace/encoder"

	"github.com/anjor/anelace/internal/anelace/util/argparser"
	"github.com/anjor/anelace/internal/constants"
	"github.com/anjor/anelace/internal/util/stream"
	"github.com/anjor/anelace/internal/util/text"
	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
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
}

const (
	statsBlocks = 1 << iota
	statsRingbuf
)

type emissionTargets map[string]io.Writer

const (
	emNone        = "none"
	emStatsText   = "stats-text"
	emStatsJsonl  = "stats-jsonl"
	emRootsJsonl  = "roots-jsonl"
	emCarV1Stream = "car-v1-stream"
)

// where the CLI initial error messages go
var argParseErrOut = os.Stderr

func NewFromArgv(argv []string) (anl *Anelace) {

	anl = NewAnelace()

	// populate args
	{
		s := &anl.statSummary
		s.SysStats.ArgvInitial = make([]string, len(argv)-1)
		copy(s.SysStats.ArgvInitial, argv[1:])
	}

	cfg := &anl.cfg
	cfg.initArgvParser()

	// accumulator for multiple errors, to present to the user all at once
	argParseErrs := argparser.Parse(argv, cfg.optSet)

	if cfg.Help || cfg.HelpAll {
		cfg.printUsage()
		os.Exit(0)
	}

	// has a default
	if cfg.HashBits < 128 || (cfg.HashBits%8) != 0 {
		argParseErrs = append(argParseErrs, "The value of --hash-bits must be a minimum of 128 and be divisible by 8")
	}

	if cfg.InlineMaxSize < 0 ||
		(cfg.InlineMaxSize > 0 && cfg.InlineMaxSize < 4) ||
		cfg.InlineMaxSize > constants.MaxLeafPayloadSize {
		// https://github.com/multiformats/cid/issues/21
		argParseErrs = append(argParseErrs, fmt.Sprintf(
			"--inline-max-size '%s' out of bounds 0 or [4:%d]",
			text.Commify(cfg.InlineMaxSize),
			constants.MaxLeafPayloadSize,
		))
	}

	// Parses/creates the blockmaker/nodeencoder, to pass in turn to the collector chain
	// Not stored in the anl object itself, to cut down on logic leaks
	nodeEnc, errorMessages := anl.setupEncoding()
	argParseErrs = append(argParseErrs, errorMessages...)
	argParseErrs = append(argParseErrs, anl.setupChunker()...)
	argParseErrs = append(argParseErrs, anl.setupCollector(nodeEnc)...)
	argParseErrs = append(argParseErrs, anl.setupEmitters()...)

	// Opts check out - set up the car emitter
	if len(argParseErrs) == 0 && anl.cfg.emitters[emCarV1Stream] != nil {
		argParseErrs = append(argParseErrs, anl.setupCarWriting()...)
	}

	if len(argParseErrs) != 0 {
		fmt.Fprint(argParseErrOut, "\nFatal error parsing arguments:\n\n")
		cfg.printUsage()

		sort.Strings(argParseErrs)
		fmt.Fprintf(
			argParseErrOut,
			"Fatal error parsing arguments:\n\t%s\n",
			strings.Join(argParseErrs, "\n\t"),
		)
		os.Exit(2)
	}

	// Opts *still* check out - take a snapshot of what we ended up with

	// All cid-determining opt come last in a predefined order
	cidOpts := []string{
		"inline-max-size",
		"hash",
		"hash-bits",
		"chunker",
		"collector",
		"node-encoder",
	}
	cidOptsIdx := map[string]struct{}{}
	for _, n := range cidOpts {
		cidOptsIdx[n] = struct{}{}
	}

	// first do the generic options
	cfg.optSet.VisitAll(func(o getopt.Option) {
		switch o.LongName() {
		case "help", "help-all":
			// do nothing for these
		default:
			// skip these keys too, they come next
			if _, exists := cidOptsIdx[o.LongName()]; !exists {
				anl.statSummary.SysStats.ArgvExpanded = append(
					anl.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
						o.LongName(),
						o.Value().String(),
					),
				)
			}
		}
	})
	sort.Strings(anl.statSummary.SysStats.ArgvExpanded)

	// now do the remaining cid-determining options
	for _, n := range cidOpts {
		anl.statSummary.SysStats.ArgvExpanded = append(
			anl.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
				n,
				cfg.optSet.GetValue(n),
			),
		)
	}

	return
}

func (cfg *config) printUsage() {
	cfg.optSet.PrintUsage(argParseErrOut)
	if cfg.HelpAll || len(cfg.erroredChunkers) > 0 || len(cfg.erroredCollectors) > 0 {
		printPluginUsage(
			argParseErrOut,
			cfg.erroredCollectors,
			cfg.erroredNodeEncoders,
			cfg.erroredChunkers,
		)
	} else {
		fmt.Fprint(argParseErrOut, "\nTry --help-all for more info\n\n")
	}
}

func printPluginUsage(
	out io.Writer,
	listCollectors []string,
	listNodeEncoders []string,
	listChunkers []string,
) {

	// if nothing was requested explicitly - list everything
	if len(listCollectors) == 0 && len(listNodeEncoders) == 0 && len(listChunkers) == 0 {
		for name, initializer := range availableCollectors {
			if initializer != nil {
				listCollectors = append(listCollectors, name)
			}
		}
		for name, initializer := range availableNodeEncoders {
			if initializer != nil {
				listNodeEncoders = append(listNodeEncoders, name)
			}
		}
		for name, initializer := range availableChunkers {
			if initializer != nil {
				listChunkers = append(listChunkers, name)
			}
		}
	}

	if len(listCollectors) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listCollectors)
		for _, name := range listCollectors {
			fmt.Fprintf(
				out,
				"[C]ollector '%s'\n",
				name,
			)
			_, h := availableCollectors[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	if len(listNodeEncoders) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listNodeEncoders)
		for _, name := range listNodeEncoders {
			fmt.Fprintf(
				out,
				"[N]odeEncoder '%s'\n",
				name,
			)
			_, h := availableNodeEncoders[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	if len(listChunkers) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listChunkers)
		for _, name := range listChunkers {
			fmt.Fprintf(
				out,
				"[C]hunker '%s'\n",
				name,
			)
			_, _, h := availableChunkers[name](nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	fmt.Fprint(out, "\n")
}

func (cfg *config) initArgvParser() {
	// The default documented way of using pborman/options is to muck with globals
	// Operate over objects instead, allowing us to re-parse argv multiple times
	o := getopt.New()
	if err := options.RegisterSet("", cfg, o); err != nil {
		log.Fatalf("option set registration failed: %s", err)
	}
	cfg.optSet = o

	// program does not take freeform args
	// need to override this for sensible help render
	o.SetParameters("")

	// Several options have the help-text assembled programmatically
	o.FlagLong(&cfg.hashFunc, "hash", 0, "Hash function to use, one of: "+text.AvailableMapKeys(anlblock.AvailableHashers),
		"algname",
	)
	o.FlagLong(&cfg.requestedNodeEncoder, "node-encoder", 0, "The IPLD-ish node encoder to use, one of: "+text.AvailableMapKeys(availableNodeEncoders),
		"encname_opt1_opt2_..._optN",
	)
	o.FlagLong(&cfg.requestedChunker, "chunker", 0,
		"Stream chunking algorithm chain. One of: "+text.AvailableMapKeys(availableChunkers),
		"chname_opt1_opt2_..._optN",
	)
	o.FlagLong(&cfg.requestedCollector, "collector", 0,
		"Node-forming algorithm chain. One of: "+text.AvailableMapKeys(availableCollectors),
		"colname_opt1_opt2_..._optN",
	)
	o.FlagLong(&cfg.emittersStdErr, "emit-stderr", 0, fmt.Sprintf(
		"One or more emitters to activate on stdERR. Available emitters are %s. Default: ",
		text.AvailableMapKeys(cfg.emitters),
	), "comma,sep,emitters")
	o.FlagLong(&cfg.emittersStdOut, "emit-stdout", 0,
		"One or more emitters to activate on stdOUT. Available emitters same as above. Default: ",
		"comma,sep,emitters",
	)
}

func (anl *Anelace) setupEmitters() (argErrs []string) {

	activeStderr := make(map[string]bool, len(anl.cfg.emittersStdErr))
	for _, s := range anl.cfg.emittersStdErr {
		activeStderr[s] = true
		if val, exists := anl.cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("invalid emitter '%s' specified for --emit-stderr. Available emitters are: %s",
				s,
				text.AvailableMapKeys(anl.cfg.emitters),
			))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			anl.cfg.emitters[s] = os.Stderr
		}
	}
	activeStdout := make(map[string]bool, len(anl.cfg.emittersStdOut))
	for _, s := range anl.cfg.emittersStdOut {
		activeStdout[s] = true
		if val, exists := anl.cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("invalid emitter '%s' specified for --emit-stdout. Available emitters are: %s",
				s,
				text.AvailableMapKeys(anl.cfg.emitters),
			))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			anl.cfg.emitters[s] = os.Stdout
		}
	}

	for _, exclusiveEmitter := range []string{
		emNone,
		emStatsText,
		emCarV1Stream,
	} {
		if activeStderr[exclusiveEmitter] && len(activeStderr) > 1 {
			argErrs = append(argErrs, fmt.Sprintf(
				"When specified, emitter '%s' must be the sole argument to --emit-stderr",
				exclusiveEmitter,
			))
		}
		if activeStdout[exclusiveEmitter] && len(activeStdout) > 1 {
			argErrs = append(argErrs, fmt.Sprintf(
				"When specified, emitter '%s' must be the sole argument to --emit-stdout",
				exclusiveEmitter,
			))
		}
	}

	// set shortcuts based on emitter config
	anl.generateRoots = anl.cfg.emitters[emRootsJsonl] != nil || anl.cfg.emitters[emStatsJsonl] != nil

	return
}

func (anl *Anelace) setupCarWriting() (argErrs []string) {

	if (anl.cfg.StatsActive & statsBlocks) != statsBlocks {
		argErrs = append(argErrs, "disabling blockstat collection conflicts with streaming .car data")
	}

	if stream.IsTTY(anl.cfg.emitters[emCarV1Stream]) {
		argErrs = append(argErrs, "output of .car streams to a TTY is not supported")
	}

	if len(argErrs) > 0 {
		return
	}

	anl.carDataWriter = anl.cfg.emitters[emCarV1Stream]

	if f, isFh := anl.carDataWriter.(*os.File); isFh {
		if s, err := f.Stat(); err != nil {
			log.Printf("Failed to stat() the car stream output: %s", err)
		} else {
			for _, opt := range stream.WriteOptimizations {
				if err := opt.Action(f, s); err != nil && err != os.ErrInvalid {
					log.Printf("Failed to apply write optimization hint '%s' to car stream output: %s\n", opt.Name, err)
				}
			}
		}
	}

	return
}

// Parses/creates the blockmaker/nodeencoder, to pass in turn to the collector chain
// Not stored in the anl object itself, to cut down on logic leaks
func (anl *Anelace) setupEncoding() (nodeEnc anlencoder.NodeEncoder, argErrs []string) {

	cfg := anl.cfg

	var blockMaker anlblock.Maker
	if _, exists := anlblock.AvailableHashers[cfg.hashFunc]; !exists {

		argErrs = append(argErrs, fmt.Sprintf(
			"Hash function '%s' requested via '--hash=algname' is not valid. Available hash names are %s",
			cfg.hashFunc,
			text.AvailableMapKeys(anlblock.AvailableHashers),
		))
	} else {

		if cfg.hashFunc == "none" && !cfg.optSet.IsSet("async-hashers") {
			cfg.AsyncHashers = 0
		}

		var errStr string
		blockMaker, anl.asyncHashingBus, errStr = anlblock.MakerFromConfig(
			cfg.hashFunc,
			cfg.HashBits/8,
			cfg.InlineMaxSize,
			cfg.AsyncHashers,
		)
		if errStr != "" {
			argErrs = append(argErrs, errStr)
		}
	}

	// bail if we couldn't init a blockmaker
	if len(argErrs) > 0 {
		return
	}

	// setup the formatter
	var b32Encoder *base32.Encoding
	if anl.cfg.CidMultibase == "base32" {
		b32Encoder = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)
	} else if anl.cfg.CidMultibase != "base36" {
		argErrs = append(argErrs, fmt.Sprintf("unsupported cid multibase '%s'", anl.cfg.CidMultibase))
		return
	}

	anl.formattedCid = func(h *anlblock.Header) (cs string) {

		if h == nil {
			return "N/A"
		}

		if b32Encoder != nil {
			cs = "b" + b32Encoder.EncodeToString(h.Cid())
		} else {
			cs = "k" + base36.EncodeToStringLc(h.Cid())
		}

		// construct something usable for both humans and cid-decoders
		if h.DummyHashed() {
			b := []byte(cs)
			copy(b[10:], "zzzznohash")
			for i := 20; i < len(b); i++ {
				b[i] = 'z'
			}
			cs = string(b)
		}

		return
	}

	nodeEncArgs := strings.Split(cfg.requestedNodeEncoder, "_")
	if init, exists := availableNodeEncoders[nodeEncArgs[0]]; !exists {
		argErrs = append(argErrs, fmt.Sprintf(
			"Encoder '%s' not found. Available encoder names are: %s",
			nodeEncArgs[0],
			text.AvailableMapKeys(availableNodeEncoders),
		))
	} else {
		for n := range nodeEncArgs {
			if n > 0 {
				nodeEncArgs[n] = "--" + nodeEncArgs[n]
			}
		}

		var initErrors []string
		if nodeEnc, initErrors = init(
			nodeEncArgs,
			&anlencoder.AnlConfig{
				BlockMaker: blockMaker,
				HasherName: cfg.hashFunc,
				HasherBits: cfg.HashBits,
				NewLinkBlockCallback: func(newLinkHdr *anlblock.Header) {
					anl.asyncWG.Add(1)
					go anl.postProcessBlock(
						newLinkHdr,
						nil, // a link-node has no data, for now at least
					)
				},
			},
		); len(initErrors) > 0 {
			cfg.erroredNodeEncoders = append(cfg.erroredNodeEncoders, nodeEncArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of node encoder '%s' failed: %s",
					nodeEncArgs[0],
					e,
				))
			}
		}
	}

	return
}

func (anl *Anelace) setupChunker() (argErrs []string) {

	if anl.cfg.requestedChunker == "" {
		return []string{
			"You must specify a stream chunker via '--chunker=algname1_opt1_opt2...'. Available chunker names are: " +
				text.AvailableMapKeys(availableChunkers),
		}
	}

	chunkerArgs := strings.Split(anl.cfg.requestedChunker, "_")
	init, exists := availableChunkers[chunkerArgs[0]]
	if !exists {
		return []string{
			fmt.Sprintf(
				"Chunker '%s' not found. Available chunker names are: %s",
				chunkerArgs[0],
				text.AvailableMapKeys(availableChunkers),
			),
		}
	}

	for n := range chunkerArgs {
		if n > 0 {
			chunkerArgs[n] = "--" + chunkerArgs[n]
		}
	}

	chunkerInstance, chunkerConstants, initErrors := init(chunkerArgs)

	if len(initErrors) == 0 {
		if chunkerConstants.MaxChunkSize < 1 || chunkerConstants.MaxChunkSize > constants.MaxLeafPayloadSize {
			initErrors = append(initErrors, fmt.Sprintf(
				"returned MaxChunkSize constant '%d' out of range [1:%d]",
				chunkerConstants.MaxChunkSize,
				constants.MaxLeafPayloadSize,
			))
		} else if chunkerConstants.MinChunkSize < 0 || chunkerConstants.MinChunkSize > chunkerConstants.MaxChunkSize {
			initErrors = append(initErrors, fmt.Sprintf(
				"returned MinChunkSize constant '%d' out of range [0:%d]",
				chunkerConstants.MinChunkSize,
				chunkerConstants.MaxChunkSize,
			))
		}
	}

	if len(initErrors) > 0 {
		anl.cfg.erroredChunkers = append(anl.cfg.erroredChunkers, chunkerArgs[0])
		for _, e := range initErrors {
			argErrs = append(argErrs, fmt.Sprintf(
				"Initialization of chunker '%s' failed: %s",
				chunkerArgs[0],
				e,
			))
		}
		return
	}

	anl.chunker = chunkerUnit{
		instance:  chunkerInstance,
		constants: chunkerConstants,
	}

	return
}

func (anl *Anelace) setupCollector(nodeEnc anlencoder.NodeEncoder) (argErrs []string) {

	if anl.cfg.optSet.IsSet("collector") && anl.cfg.requestedCollector == "" {
		return []string{
			"When specified, collector arg must be in the form '--collector=algname_opt1_opt2...'. Available collector names are: " +
				text.AvailableMapKeys(availableCollectors),
		}
	}

	collectorArgs := strings.Split(anl.cfg.requestedCollector, "_")
	init, exists := availableCollectors[collectorArgs[0]]
	if !exists {
		return []string{
			fmt.Sprintf(
				"Collector '%s' not found. Available collector names are: %s",
				collectorArgs[0],
				text.AvailableMapKeys(availableCollectors),
			),
		}
	}

	for n := range collectorArgs {
		if n > 0 {
			collectorArgs[n] = "--" + collectorArgs[n]
		}
	}

	collectorInstance, initErrors := init(
		collectorArgs,
		&anlcollector.AnlConfig{NodeEncoder: nodeEnc},
	)

	if len(initErrors) > 0 {
		anl.cfg.erroredCollectors = append(anl.cfg.erroredCollectors, collectorArgs[0])
		for _, e := range initErrors {
			argErrs = append(argErrs, fmt.Sprintf(
				"Initialization of collector '%s' failed: %s",
				collectorArgs[0],
				e,
			))
		}
		return
	}

	anl.collector = collectorInstance
	return
}

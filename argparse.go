package anelace

import (
	"encoding/base32"
	"fmt"
	"github.com/anjor/anelace/internal/block"
	"github.com/anjor/anelace/internal/collector"
	"github.com/anjor/anelace/internal/constants"
	"github.com/anjor/anelace/internal/encoder"
	"github.com/anjor/anelace/internal/util/stream"
	"github.com/anjor/anelace/internal/util/text"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/multiformats/go-base36"

	"github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

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

func inlineMaxSizeWithinBounds(ims int) bool {
	return ims == 0 || (ims >= 4 && ims < constants.MaxLeafPayloadSize)
}

type compatIpfsArgs struct {
	CidVersion       int    `getopt:"--cid-version"`
	InlineActive     bool   `getopt:"--inline"`
	InlineLimit      int    `getopt:"--inline-limit"`
	UseRawLeaves     bool   `getopt:"--raw-leaves"`
	UpgradeV0CID     bool   `getopt:"--upgrade-cidv0-in-output"`
	TrickleCollector bool   `getopt:"--trickle"`
	Chunker          string `getopt:"--chunker"`
	Hasher           string `getopt:"--hash"`
}

func (cfg *config) presetFromIPFS() (parseErrors []string) {

	lVals, optSet := options.RegisterNew("", &compatIpfsArgs{
		Chunker:    "size",
		CidVersion: 0,
	})
	ipfsOpts := lVals.(*compatIpfsArgs)

	args := append([]string{"ipfs-compat"}, strings.Split(cfg.IpfsCompatCmd, " ")...)
	for {
		if err := optSet.Getopt(args, nil); err != nil {
			parseErrors = append(parseErrors, err.Error())
		}
		args = optSet.Args()
		if len(args) == 0 {
			break
		} else if args[0] != "" && args[0] != "add" { // next iteration will eat the chaff as a "progname"
			parseErrors = append(parseErrors, fmt.Sprintf(
				"unexpected ipfs-compatible parameter(s): %s...",
				args[0],
			))
		}
	}

	// bail early if errors present already
	if len(parseErrors) > 0 {
		return parseErrors
	}

	if !cfg.optSet.IsSet("hash") {
		if !optSet.IsSet("hash") {
			cfg.hashFunc = "sha2-256"
		} else {
			cfg.hashFunc = ipfsOpts.Hasher
		}
	}

	if !cfg.optSet.IsSet("inline-max-size") {
		if ipfsOpts.InlineActive {
			if optSet.IsSet("inline-limit") {
				cfg.InlineMaxSize = ipfsOpts.InlineLimit
			} else {
				cfg.InlineMaxSize = 32
			}
		} else {
			cfg.InlineMaxSize = 0
		}
	}

	// ignore everything compat if a collector is already given
	if !cfg.optSet.IsSet("collector") {
		// either trickle or fixed-outdegree, go-ipfs doesn't understand much else
		if ipfsOpts.TrickleCollector {
			cfg.requestedCollector = "trickle_max-direct-leaves=174_max-sibling-subgroups=4_unixfs-nul-leaf-compat"
		} else {
			cfg.requestedCollector = "fixed-outdegree_max-outdegree=174"
		}
	}

	// ignore everything compat if an encoder is already given
	if !cfg.optSet.IsSet("node-encoder") {

		ufsv1EncoderOpts := []string{"unixfsv1", "merkledag-compat-protobuf"}

		if ipfsOpts.CidVersion != 1 {
			if ipfsOpts.UpgradeV0CID && ipfsOpts.CidVersion == 0 {
				ufsv1EncoderOpts = append(ufsv1EncoderOpts, "cidv0")
			} else {
				parseErrors = append(
					parseErrors,
					fmt.Sprintf("--cid-version=%d is unsupported ( try --cid-version=1 or --upgrade-cidv0-in-output )", ipfsOpts.CidVersion),
				)
			}
		} else if !optSet.IsSet("raw-leaves") {
			ipfsOpts.UseRawLeaves = true
		}

		if !ipfsOpts.UseRawLeaves {
			if ipfsOpts.TrickleCollector {
				ufsv1EncoderOpts = append(ufsv1EncoderOpts, "unixfs-leaf-decorator-type=0")
			} else {
				ufsv1EncoderOpts = append(ufsv1EncoderOpts, "unixfs-leaf-decorator-type=2")
			}
		}

		cfg.requestedNodeEncoder = strings.Join(ufsv1EncoderOpts, "_")
	}

	// ignore everything compat if a chunker is already given
	if !cfg.optSet.IsSet("chunker") {

		if strings.HasPrefix(ipfsOpts.Chunker, "size") {
			sizeopts := strings.Split(ipfsOpts.Chunker, "-")
			if sizeopts[0] == "size" {
				if len(sizeopts) == 1 {
					cfg.requestedChunker = "fixed-size_262144"
				} else if len(sizeopts) == 2 {
					cfg.requestedChunker = "fixed-size_" + sizeopts[1]
				}
			}
		} else if strings.HasPrefix(ipfsOpts.Chunker, "rabin") {
			rabinopts := strings.Split(ipfsOpts.Chunker, "-")
			if rabinopts[0] == "rabin" {

				var bits, min, max string

				if len(rabinopts) == 1 {
					bits = "18"
					min = "87381"  // (2**18)/3
					max = "393216" // (2**18)+(2**18)/2

				} else if len(rabinopts) == 2 {
					if avg, err := strconv.ParseUint(rabinopts[1], 10, 64); err == nil {
						bits = fmt.Sprintf("%d", int(math.Log2(float64(avg))))
						min = fmt.Sprintf("%d", avg/3)
						max = fmt.Sprintf("%d", avg+avg/2)
					}

				} else if len(rabinopts) == 4 {
					if avg, err := strconv.ParseUint(rabinopts[2], 10, 64); err == nil {
						bits = fmt.Sprintf("%d", int(math.Log2(float64(avg))))
						min = rabinopts[1]
						max = rabinopts[3]
					}
				}

				if bits != "" {
					cfg.requestedChunker = fmt.Sprintf("rabin_polynomial=17437180132763653_window-size=16_state-target=0_state-mask-bits=%s_min-size=%s_max-size=%s",
						bits,
						min,
						max,
					)
				}
			}
		} else if strings.HasPrefix(ipfsOpts.Chunker, "buzhash") {
			buzopts := strings.Split(ipfsOpts.Chunker, "-")
			if buzopts[0] == "buzhash" {
				if len(buzopts) == 1 {
					cfg.requestedChunker = "buzhash_hash-table=GoIPFSv0_state-target=0_state-mask-bits=17_min-size=131072_max-size=524288"
				}
			}
		}

		if cfg.requestedChunker == "" {
			parseErrors = append(
				parseErrors,
				fmt.Sprintf("Invalid ipfs-compatible spec --chunker=%s", ipfsOpts.Chunker),
			)
		}
	}

	return parseErrors
}

package main

import (
	"fmt"
	"github.com/anjor/anelace/internal/util/stream"
	"github.com/anjor/anelace/pkg"
	"log"
	"os"
)

func main() {

	inStat, statErr := os.Stdin.Stat()
	if statErr != nil {
		log.Fatalf("unexpected error stat()ing stdIN: %s", statErr)
	}

	// Parse CLI and initialize everything
	// On error it will log.Fatal() on its own
	anl := anelace.NewFromArgv(os.Args)

	if stream.IsTTY(os.Stdin) {
		fmt.Fprint(
			os.Stderr,
			"------\nYou seem to be feeding data straight from a terminal, an odd choice...\nNevertheless will proceed to read until EOF ( Ctrl+D )\n------\n",
		)
	} else if !inStat.Mode().IsRegular() || inStat.Size() > 16*1024*1024 { // SANCHECK - arbitrary
		// Try optimizations if:
		// - not a reguar file (and not a TTY - exempted above)
		// - regular file larger than a certain size (SANCHECK: somewhat arbitrary)
		// An optimization returns os.ErrInvalid when it can't be applied to the file type
		for _, opt := range stream.ReadOptimizations {
			if err := opt.Action(os.Stdin, inStat); err != nil && err != os.ErrInvalid {
				log.Printf("Failed to apply read optimization hint '%s' to stdIN: %s\n", opt.Name, err)
			}
		}
	}

	processErr := anl.ProcessReader(
		os.Stdin,
		nil,
	)
	anl.Destroy()
	if processErr != nil {
		log.Fatalf("Unexpected error processing STDIN: %s", processErr)
	}

	anl.OutputSummary()
}

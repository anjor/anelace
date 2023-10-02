package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/anjor/anelace"
	"github.com/anjor/anelace/internal/util/stream"
)

func TestDeterministicCarContent(t *testing.T) {

	const TEST_ITERATIONS = 10

	var first [32]byte
	for iter := 0; iter < TEST_ITERATIONS; iter++ {
		inStat, statErr := os.Stdin.Stat()
		if statErr != nil {
			t.Fatalf("unexpected error stat-ing stdin: %s", statErr)
		}

		mockStderr, mockStdout := new(bytes.Buffer), new(bytes.Buffer)

		anl, errs := anelace.NewAnelaceWithWriters(mockStderr, mockStdout)
		if len(errs) > 0 {
			for _, err := range errs {
				fmt.Println(err)
				t.Error(err)
			}
		}

		var err error

		//mockOsStdin, err := os.CreateTemp("", "*")
		mockOsStdin, err := os.Open("/Users/jay/Documents/code/anelace/bin/sample.txt")
		if err != nil {
			fmt.Printf("Error: %s", err)
		}

		for _, opt := range stream.ReadOptimizations {
			if err := opt.Action(mockOsStdin, inStat); err != nil && err != os.ErrInvalid {
				log.Printf("Failed to apply read optimization hint '%s' to stdIN: %s\n", opt.Name, err)
			}
		}

		processErr := anl.ProcessReader(
			mockOsStdin,
			nil,
		)
		anl.Destroy()
		if processErr != nil {
			log.Fatalf("Unexpected error processing STDIN: %s", processErr)
		}

		if iter == 0 {
			first = sha256.Sum256(mockStdout.Bytes())
		} else {
			current := sha256.Sum256(mockStdout.Bytes())
			if current != first {
				t.Errorf("iteration %d: content sum does not match first content sum on iteration [ %s, %s ]", iter, hex.EncodeToString(first[:]), hex.EncodeToString(current[:]))
			}
		}
	}
}

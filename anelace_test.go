package anelace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestDeterministicCarContent(t *testing.T) {

	const TEST_ITERATIONS = 10

	var first [32]byte
	for iter := 0; iter < TEST_ITERATIONS; iter++ {
		mockStderr, mockStdout := new(bytes.Buffer), new(bytes.Buffer)
		anl, errs := NewAnelaceWithWriters(mockStderr, mockStdout)
		if len(errs) > 0 {
			for _, err := range errs {
				fmt.Println(err)
				t.Error(err)
			}
		}

		mockOsStdin, err := os.Open("test/sample-payload.dat")
		if err != nil {
			fmt.Printf("Error: %s", err)
		}

		// process the mock stdin input
		processErr := anl.ProcessReader(
			mockOsStdin,
			nil,
		)
		anl.Destroy()
		if processErr != nil {
			log.Fatalf("Unexpected error processing STDIN: %s", processErr)
		}

		// check to see if the sums match
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

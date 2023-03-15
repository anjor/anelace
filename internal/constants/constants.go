package constants

import (
	"os"
	"strconv"
)

const (
	// https://github.com/ipfs/go-ipfs-chunker/pull/21#discussion_r369197120
	MaxLeafPayloadSize = 1024 * 1024
	MaxBlockWireSize   = (2 * 1024 * 1024) - 1
)

type Incomparabe [0]func()

var LongTests bool
var VeryLongTests bool

func init() {
	VeryLongTests = isTruthy("TEST_ANELACE_VERY_LONG")
	LongTests = VeryLongTests || isTruthy("TEST_ANELACE_LONG")
}

func isTruthy(varname string) bool {
	envStr := os.Getenv(varname)
	if envStr != "" {
		if num, err := strconv.ParseUint(envStr, 10, 64); err != nil || num != 0 {
			return true
		}
	}
	return false
}

var PerformSanityChecks = true

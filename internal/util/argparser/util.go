package argparser

import (
	"bytes"
	"fmt"
	"github.com/anjor/anelace/internal/constants"
	"reflect"
	"regexp"
	"strconv"

	"github.com/pborman/getopt/v2"
)

// ugly as sin due to lack of lookaheads :/
var indenter = regexp.MustCompile(`(?m)^([^\n])`)
var nonOptIndenter = regexp.MustCompile(`(?m)^\s{0,12}([^\s\n\-])`)
var dashStripper = regexp.MustCompile(`(?m)^(\s*)\-\-`)

func SubHelp(description string, optSet *getopt.Set) (sh []error) {

	sh = append(
		sh,
		fmt.Errorf(string(indenter.ReplaceAll(
			[]byte(description),
			[]byte(`  $1`),
		))),
	)

	if optSet == nil {
		return sh
	}

	b := bytes.NewBuffer(make([]byte, 0, 1024))
	optSet.PrintOptions(b)

	sh = append(sh, fmt.Errorf("  ------------\n   SubOptions"))
	sh = append(sh,
		fmt.Errorf(string(dashStripper.ReplaceAll(
			nonOptIndenter.ReplaceAll(
				b.Bytes(),
				[]byte(`              $1`),
			),
			[]byte(`$1  `),
		))),
	)

	return sh
}

var maxPlaceholder = regexp.MustCompile(`\bMaxPayload\b`)

func Parse(args []string, optSet *getopt.Set) (argErrs []error) {

	if err := optSet.Getopt(args, nil); err != nil {
		argErrs = append(argErrs, err)
	}

	unexpectedArgs := optSet.Args()
	if len(unexpectedArgs) != 0 {
		argErrs = append(argErrs, fmt.Errorf(
			"unexpected free-form parameter(s): %s...",
			unexpectedArgs[0],
		))
	}

	// going through the limits when we are already in error is too confusing
	if len(argErrs) > 0 {
		return
	}

	optSet.VisitAll(func(o getopt.Option) {
		if spec := []byte(reflect.ValueOf(o).Elem().FieldByName("name").String()); len(spec) > 0 {

			max := int((^uint(0)) >> 1)
			min := -max - 1

			if spec[0] == '[' && spec[len(spec)-1] == ']' {
				spec = maxPlaceholder.ReplaceAll(spec, []byte(fmt.Sprintf("%d", constants.MaxLeafPayloadSize)))

				if _, err := fmt.Sscanf(string(spec), "[%d:]", &min); err != nil {
					if _, err := fmt.Sscanf(string(spec), "[%d:%d]", &min, &max); err != nil {
						argErrs = append(argErrs, fmt.Errorf("Failed parsing '%s' as '[%%d:%%d]' - %s", spec, err))
						return
					}
				}
			} else {
				// not a spec we recognize
				return
			}

			if !o.Seen() {
				argErrs = append(argErrs, fmt.Errorf("a value for %s must be specified", o.LongName()))
				return
			}

			actual, err := strconv.ParseInt(o.Value().String(), 10, 64)
			if err != nil {
				argErrs = append(argErrs, err)
				return
			}

			if actual < int64(min) || actual > int64(max) {
				argErrs = append(argErrs, fmt.Errorf(
					"value '%d' supplied for %s out of range [%d:%d]",
					actual,
					o.LongName(),
					min, max,
				))
			}
		}
	})

	return
}

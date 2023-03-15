//go:build !windows
// +build !windows

package pkg

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func init() {

	preProcessTasks = func(anl *Anelace) {
		var ru unix.Rusage
		unix.Getrusage(unix.RUSAGE_SELF, &ru) //nolint:errcheck
		sys := &anl.statSummary.SysStats

		// set everything to negative values: we will simply += in postprocessing
		sys.CpuUserNsecs -= unix.TimevalToNsec(ru.Utime)
		sys.CpuSysNsecs -= unix.TimevalToNsec(ru.Stime)
		sys.MinFlt -= int64(ru.Minflt)
		sys.MajFlt -= int64(ru.Majflt)
		sys.BioRead -= int64(ru.Inblock)
		sys.BioWrite -= int64(ru.Oublock)
		sys.Sigs -= int64(ru.Nsignals)
		sys.CtxSwYield -= int64(ru.Nvcsw)
		sys.CtxSwForced -= int64(ru.Nivcsw)
	}

	postProcessTasks = func(anl *Anelace) {
		var ru unix.Rusage
		unix.Getrusage(unix.RUSAGE_SELF, &ru) //nolint:errcheck

		if runtime.GOOS != "darwin" {
			// anywhere but mac, maxrss is actually KiB, because fuck standards
			// https://lists.apple.com/archives/darwin-kernel/2009/Mar/msg00005.html
			ru.Maxrss *= 1024
		}

		sys := &anl.statSummary.SysStats

		sys.MaxRssBytes = int64(ru.Maxrss)
		sys.CpuUserNsecs += unix.TimevalToNsec(ru.Utime)
		sys.CpuSysNsecs += unix.TimevalToNsec(ru.Stime)
		sys.MinFlt += int64(ru.Minflt)
		sys.MajFlt += int64(ru.Majflt)
		sys.BioRead += int64(ru.Inblock)
		sys.BioWrite += int64(ru.Oublock)
		sys.Sigs += int64(ru.Nsignals)
		sys.CtxSwYield += int64(ru.Nvcsw)
		sys.CtxSwForced += int64(ru.Nivcsw)
	}
}

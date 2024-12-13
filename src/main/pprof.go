package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/sirupsen/logrus"
)

func activateProfiling(dir string, duration time.Duration) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700)
	}

	f, err := os.Create(fmt.Sprintf("%s/%s.pprof", dir, time.Now().Format("2006-01-02_15:04:05")))
	if err != nil {
		logrus.Debugf("Failed to open file for profiling: %+v", err)
		return
	}

	logrus.Infof("Profiling start!")

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	time.Sleep(duration)

	logrus.Infof("Profiling done!")
}

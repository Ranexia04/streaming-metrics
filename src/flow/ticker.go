package flow

import (
	"time"

	"example.com/streaming-metrics/src/store"
)

var metricManagers = make(map[string]*store.MetricManager)

func Ticker(granularity int64, namespaces map[string]*Namespace) {
	ticker := time.NewTicker(time.Duration(granularity) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		store.SyncTime = time.Now()
		for _, metricManager := range metricManagers {
			metricManager.Tick()
		}
	}
}

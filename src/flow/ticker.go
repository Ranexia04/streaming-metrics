package flow

import (
	"time"

	"example.com/streaming-metrics/src/prom"
	"example.com/streaming-metrics/src/store"
)

func Ticker(cardinality int64, namespaces map[string]*Namespace) {
	ticker := time.NewTicker(time.Duration(cardinality) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		store.SyncTime = time.Now()
		for _, windowManager := range prom.WindowManagers {
			windowManager.Tick()
		}
	}
}

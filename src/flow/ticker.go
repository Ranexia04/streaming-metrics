package flow

import (
	"time"

	"example.com/streaming-metrics/src/store"
)

func Ticker(granularity int64) {
	ticker := time.NewTicker(time.Duration(granularity) * time.Second)
	defer ticker.Stop()

	for {
		store.SyncTime = time.Now()
		requestCount.Tick()
		requestDuration.Tick()
		<-ticker.C
	}
}

package flow

import (
	"time"
)

func Ticker(cardinality int64, namespaces map[string]*Namespace) {
	ticker := time.NewTicker(time.Duration(cardinality) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		for _, namespace := range namespaces {
			namespace.Store.Tick()
		}
	}
}

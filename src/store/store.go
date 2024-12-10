package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"example.com/streaming-metrics/src/prom"
	"github.com/itchyny/gojq"
)

type MemoryStore struct {
	granularity int64
	cardinality int64
	windows     map[string]*Window

	rwMutex sync.RWMutex
}

func NewMemoryStore(metrics map[string]*prom.Metric, granularity int64, cardinality int64) *MemoryStore {
	if !validateConfigs(granularity, cardinality) {
		return nil
	}

	memoryStore := &MemoryStore{
		granularity: granularity,
		cardinality: cardinality,
		windows:     make(map[string]*Window),
	}

	for _, metric := range metrics {
		memoryStore.windows[metric.Name] = newWindow(cardinality, granularity)
	}

	return memoryStore
}

func (store *MemoryStore) Tick() {
	store.rwMutex.RLock()

	currentTime := time.Now()
	//TODO mutex arround this update (is it even necessary? - unlikely to be concurrency and temporary incorrect value is good enough)
	for _, window := range store.windows {
		window.Tick(currentTime)
	}

	store.rwMutex.RUnlock()
}

func (store *MemoryStore) Push(id string, t time.Time, metric any, lambda *gojq.Code) {
	store.rwMutex.RLock()

	window, ok := store.windows[id]
	if !ok {
		store.rwMutex.RUnlock()
		store.rwMutex.Lock()
		if _, ok := store.windows[id]; !ok {
			store.windows[id] = newWindow(store.cardinality, store.granularity)
		}
		store.rwMutex.Unlock()
		store.rwMutex.RLock()
		window = store.windows[id]
	}

	window.Push(t, metric)
	store.rwMutex.RUnlock()
}

func (store *MemoryStore) String() string {
	var builder strings.Builder

	for i, window := range store.windows {
		builder.WriteString(fmt.Sprintf("Window %s:\n%s", i, window.String()))
	}

	return builder.String()
}

func validateConfigs(granularity int64, cardinality int64) bool {
	return granularity > 0 && cardinality > 0
}

package store

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

var SyncTime time.Time = time.Now()

type Window struct {
	metricType  string
	granularity time.Duration
	cardinality int64
	buckets     []*Bucket

	mutex sync.Mutex
}

func newWindow(metricType string, cardinality int64, granularity int64) *Window {
	window := &Window{
		metricType:  metricType,
		granularity: time.Duration(granularity) * time.Second,
		cardinality: cardinality,
		buckets:     make([]*Bucket, 0, cardinality),
	}

	for i := range cardinality {
		offset := time.Duration(granularity*(i)) * time.Second
		startTime := SyncTime.Add(-offset)
		window.buckets[cardinality-i-1] = NewBucket(metricType, startTime, window.granularity)
	}

	return window
}

func (window *Window) Update(t time.Time, metric any) {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window.update(t, metric)
}

func (window *Window) update(t time.Time, metric any) {
	index, err := window.getBucketIndex(t)
	if err != nil {
		return
	}

	window.buckets[index].Update(metric)
}

func (window *Window) getBucketIndex(t time.Time) (int, error) {
	timeDiff := t.Sub(window.buckets[0].TimeRange.Start)

	indexFloat := timeDiff / window.granularity

	if indexFloat < 0 || indexFloat > time.Duration(window.cardinality-1) {
		return -1, fmt.Errorf("time did not match any window")
	}

	return int(indexFloat), nil
}

func (window *Window) Tick() {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window.tick()
}

func (window *Window) tick() {
	oldest := window.buckets[0]
	oldest.Push()
	window.buckets = window.buckets[1:]
	freshBucket := NewBucket(window.metricType, SyncTime, time.Duration(window.cardinality))
	window.buckets = append(window.buckets, freshBucket)
}

func (window *Window) String() string {
	var builder strings.Builder

	for i, bucket := range window.buckets {
		builder.WriteString(fmt.Sprintf("Bucket %d:\n%s", i, bucket.String()))
	}

	return builder.String()
}

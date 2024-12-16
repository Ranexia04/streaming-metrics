package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"example.com/streaming-metrics/src/prom"
)

var SyncTime time.Time = time.Now()

type Window struct {
	metricType  string
	granularity time.Duration
	cardinality int64
	buckets     []*Bucket
	labels      map[string]string

	mutex sync.RWMutex
}

func newWindow(labels map[string]string, metricType string, granularity int64, cardinality int64) *Window {
	window := &Window{
		metricType:  metricType,
		granularity: time.Duration(granularity) * time.Second,
		cardinality: cardinality,
		buckets:     make([]*Bucket, cardinality),
		labels:      labels,
	}

	for i := range cardinality {
		offset := time.Duration(granularity*(i)) * time.Second
		startTime := SyncTime.Add(-offset)
		window.buckets[cardinality-i-1] = NewBucket(metricType, startTime, window.granularity)
	}

	return window
}

func (window *Window) Update(t time.Time, metric any) {
	window.mutex.RLock()
	index, err := window.getBucketIndex(t)
	window.mutex.RUnlock()

	if err != nil {
		prom.IncNamespaceDiscardedMsg(window.labels["namespace"])
		return
	}

	window.mutex.Lock()
	bucket := window.buckets[index]
	window.mutex.Unlock()

	bucket.Update(metric)
}

func (window *Window) getBucketIndex(t time.Time) (int, error) {
	timeStart := window.buckets[0].TimeRange.Start
	timeEnd := window.buckets[window.cardinality-1].TimeRange.End

	if t.Before(timeStart) {
		return -1, fmt.Errorf("msg is too old")
	}

	if t.After(timeEnd) {
		return -1, fmt.Errorf("msg got here early")
	}

	durationSinceStart := t.Sub(timeStart)
	bucketIndex := int(durationSinceStart.Seconds() / window.granularity.Seconds())

	return bucketIndex, nil
}

func (window *Window) getOldestBucket() *Bucket {
	return window.buckets[0]
}

func (window *Window) Roll() {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window.roll()
}

func (window *Window) roll() {
	window.buckets = window.buckets[1:]
	freshBucket := NewBucket(window.metricType, SyncTime, window.granularity)
	window.buckets = append(window.buckets, freshBucket)
}

func (window *Window) String() string {
	var builder strings.Builder

	for i, bucket := range window.buckets {
		builder.WriteString(fmt.Sprintf("Bucket %d:\n%s", i, bucket.String()))
	}

	return builder.String()
}

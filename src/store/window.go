package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"example.com/streaming-metrics/src/prom"
)

var SyncTime time.Time

type Window struct {
	metricType  string
	granularity int64
	cardinality int64
	shift       int64
	buckets     []*Bucket
	labels      map[string]string

	mutex sync.Mutex
}

func newWindow(labels map[string]string, metricType string, granularity int64, cardinality int64, shift int64) *Window {
	window := &Window{
		metricType:  metricType,
		granularity: granularity,
		cardinality: cardinality,
		shift:       shift,
		buckets:     make([]*Bucket, cardinality),
		labels:      labels,
	}

	for i := range cardinality {
		offset := -time.Duration(granularity*(i+1-window.shift)) * time.Second
		window.buckets[cardinality-i-1] = NewBucket(metricType, SyncTime.Add(offset), time.Duration(granularity)*time.Second)
	}

	return window
}

func (window *Window) Update(t time.Time, metric any) {
	window.mutex.Lock()
	index, err := window.getBucketIndex(t)

	if err != nil {
		prom.IncNamespaceDiscardedMsg(window.labels["namespace"])
		window.mutex.Unlock()
		return
	}

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
	bucketIndex := int(durationSinceStart.Seconds() / float64(window.granularity))

	return bucketIndex, nil
}

func (window *Window) Roll() *Bucket {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	oldestBucket := window.buckets[0]
	window.buckets = window.buckets[1:]
	offset := time.Duration(window.granularity*(window.shift-1)) * time.Second
	freshBucket := NewBucket(window.metricType, SyncTime.Add(offset), time.Duration(window.granularity)*time.Second)
	window.buckets = append(window.buckets, freshBucket)
	return oldestBucket
}

func (window *Window) String() string {
	var builder strings.Builder

	for i, bucket := range window.buckets {
		builder.WriteString(fmt.Sprintf("Bucket %d:\n%s", i, bucket.String()))
	}

	return builder.String()
}

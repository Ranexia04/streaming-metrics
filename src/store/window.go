package store

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Window struct {
	granularity time.Duration
	cardinality int64
	buckets     []*Bucket

	mutex sync.Mutex
}

func newWindow(cardinality int64, granularity int64) *Window {
	window := &Window{
		granularity: time.Duration(granularity) * time.Second,
		cardinality: cardinality,
		buckets:     make([]*Bucket, cardinality),
	}

	now := time.Now()
	for i := range cardinality {
		offset := time.Duration(granularity*(i+1)) * time.Second
		beginTime := now.Add(-offset)
		window.buckets[cardinality-i-1] = NewBucket(beginTime, window.granularity)
	}

	return window
}

func (window *Window) Push(t time.Time, metric any) {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window.push(t, metric)
}

func (window *Window) push(t time.Time, metric any) {
	index := window.getBucketIndex(t)
	window.buckets[index].Push(metric)
}

func (window *Window) getBucketIndex(t time.Time) int {
	for i, bucket := range window.buckets {
		if !bucket.TimeRange.Contains(t) {
			continue
		}

		return i
	}

	return -1
}

func (window *Window) Tick(currentTime time.Time) {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window.tick(currentTime)
}

func (window *Window) tick(currentTime time.Time) {
	oldest := window.buckets[0]
	oldest.updateMetrics()
	window.buckets = window.buckets[1:]
	freshBucket := NewBucket(currentTime.Add(window.granularity), time.Duration(window.cardinality))
	window.buckets = append(window.buckets, freshBucket)
}

func (window *Window) String() string {
	var builder strings.Builder

	for i, bucket := range window.buckets {
		builder.WriteString(fmt.Sprintf("Bucket %d:\n%s", i, bucket.String()))
	}

	return builder.String()
}

package store

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

type Window struct {
	granularity time.Duration
	cardinality int64
	buckets     []Bucket

	db *pebble.DB

	currentBucketGroupKey []byte
	bucketKeys            [][]byte

	bytesNil []byte

	mutex sync.Mutex
}

func newWindow(cardinality int64, granularity int64, db *pebble.DB) *Window {
	window := &Window{
		granularity: time.Duration(granularity * int64(time.Second)),
		cardinality: cardinality,
		buckets:     make([]Bucket, 0, cardinality),

		db: db,
	}

	now := time.Now()
	for i := range cardinality {
		offset := time.Duration(granularity * (i + 1))
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
	freshBucket := NewBucket(time.Now().Add(window.granularity), time.Duration(window.cardinality))
	window.buckets[window.cardinality-1] = freshBucket
}

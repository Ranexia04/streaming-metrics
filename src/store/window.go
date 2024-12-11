package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var SyncTime time.Time = time.Now()

type Window struct {
	metricType  string
	granularity time.Duration
	cardinality int64
	buckets     []*Bucket
	labels      map[string]string

	mutex sync.Mutex
}

func newWindow(labels map[string]string, metricType string, cardinality int64, granularity int64) *Window {
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
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window.update(t, metric)
}

func (window *Window) update(t time.Time, metric any) {
	index, err := window.getBucketIndex(t)
	if err != nil {
		logrus.Errorf("not in any bucket")
		return
	}

	window.buckets[index].Update(metric)
}

func (window *Window) getBucketIndex(t time.Time) (int, error) {
	timeStart := window.buckets[0].TimeRange.Start
	timeEnd := window.buckets[window.cardinality-1].TimeRange.End

	fmt.Println(timeStart, timeEnd, t)

	if t.Before(timeStart) || t.After(timeEnd) {
		return -1, fmt.Errorf("time did not match any window")
	}

	durationSinceStart := t.Sub(timeStart)
	bucketIndex := int(durationSinceStart.Seconds() / window.granularity.Seconds())

	logrus.Infof("%d", bucketIndex)

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

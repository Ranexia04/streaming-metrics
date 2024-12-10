package store

import (
	"fmt"
	"time"
)

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (tr TimeRange) Contains(t time.Time) bool {
	return !t.Before(tr.Start) && !t.After(tr.End)
}

func (tr *TimeRange) String() string {
	return fmt.Sprintf("Time Start: %s\nTime End: %s", tr.Start, tr.End)
}

type Bucket struct {
	TimeRange TimeRange
	Data      interface{}
}

func NewBucket(beginTime time.Time, duration time.Duration) *Bucket {
	return &Bucket{
		TimeRange: TimeRange{
			Start: beginTime,
			End:   beginTime.Add(duration),
		},
		Data: 0,
	}
}

func (bucket *Bucket) Push(metric any) {
	// TODO
}

func (bucket *Bucket) updateMetrics() {
	// TODO
}

func (bucket *Bucket) String() string {
	return fmt.Sprintf("Time Range:\n%s\nData: %d\n", bucket.TimeRange.String(), bucket.Data)
}

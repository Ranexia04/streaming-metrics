package store

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
)

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (tr TimeRange) Contains(t time.Time) bool {
	return !t.Before(tr.Start) && !t.After(tr.End)
}

type Bucket struct {
	TimeRange TimeRange
	Data      interface{}
}

func NewBucket(beginTime time.Time, duration time.Duration) Bucket {
	return Bucket{
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

// TODO improve the deepcopy mecanism
func (bucket *Bucket) getRepresentation() any {
	// bucket.mutex.Lock()
	bucket_bytes, err := json.Marshal(bucket)
	// bucket.mutex.Unlock()

	if err != nil {
		logrus.Errorf("gojq_bucket marshal: %+v", err)
		return nil
	}

	bucket_rep := make(map[string]any)

	if err := json.Unmarshal(bucket_bytes, &bucket_rep); err != nil {
		logrus.Errorf("gojq_bucket unmarshal: %+v", err)
		return nil

	}

	return bucket_rep["state"]
}

func (bucket *Bucket) updateMetrics() {

}

func (bucket *Bucket) clear() {
	bucket.Data = nil
}

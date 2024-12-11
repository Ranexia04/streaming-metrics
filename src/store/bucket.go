package store

import (
	"fmt"
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

func (tr *TimeRange) String() string {
	return fmt.Sprintf("Time Start: %s\nTime End: %s", tr.Start, tr.End)
}

type Bucket struct {
	TimeRange TimeRange
	Data      any
	Update    func(any)
}

func NewBucket(metricType string, beginTime time.Time, duration time.Duration) *Bucket {
	bucket := &Bucket{
		TimeRange: TimeRange{
			Start: beginTime,
			End:   beginTime.Add(duration),
		},
		Data: initData(metricType),
	}

	var update func(any)
	switch metricType {
	case "counter":
		update = bucket.updateCounter
	case "gauge":
		update = bucket.updateGauge
	case "histogram":
		update = bucket.updateHistogram
	case "summary":
		update = bucket.updateSummary
	default:
		logrus.Errorf("Metric type %s is not allowed", metricType)
		return nil
	}

	bucket.Update = update

	return bucket
}

func (bucket *Bucket) updateCounter(metric any) {
	metricValue, ok := metric.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", metric)
		return
	}

	currentData, ok := bucket.Data.(int)
	if !ok {
		logrus.Errorf("bucket.Data %v must be type int for counter metric", bucket.Data)
		return
	}

	bucket.Data = currentData + metricValue
}

func (bucket *Bucket) updateGauge(metric any) {
	metricValue, ok := metric.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}

	currentData, ok := bucket.Data.(float64)
	if !ok {
		logrus.Errorf("bucket.Data %v must be type int for counter metric", bucket.Data)
		return
	}

	bucket.Data = currentData + metricValue
}

func (bucket *Bucket) updateHistogram(metric any) {
	metricValue, ok := metric.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for histogram metric", metricValue)
		return
	}

	currentData, ok := bucket.Data.(float64)
	if !ok {
		logrus.Errorf("bucket.Data %v must be type int for counter metric", bucket.Data)
		return
	}

	bucket.Data = currentData + metricValue
}

func (bucket *Bucket) updateSummary(metric any) {
	metricValue, ok := metric.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for summary metric", metricValue)
		return
	}

	currentData, ok := bucket.Data.(float64)
	if !ok {
		logrus.Errorf("bucket.Data %v must be type int for counter metric", bucket.Data)
		return
	}

	bucket.Data = currentData + metricValue
}

func (bucket *Bucket) String() string {
	return fmt.Sprintf("Time Range:\n%s\nData: %d\n", bucket.TimeRange.String(), bucket.Data)
}

func (bucket *Bucket) Push() {
	// TODO
}

func initData(metricType string) any {
	switch metricType {
	case "counter":
		return 0
	case "gauge":
		return 0.0
	case "histogram":
		return 0.0
	case "summary":
		return 0.0
	default:
		logrus.Errorf("Metric type %s is not allowed", metricType)
		return nil
	}
}

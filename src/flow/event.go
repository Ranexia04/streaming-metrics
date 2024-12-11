package flow

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Event struct {
	namespace string
	time      time.Time
	metrics   any
}

func NewEvent(in any) *Event {
	switch v := in.(type) {
	case map[string]any:
		namespace, ok_namespace := v["namespace"].(string)

		if !ok_namespace {
			logrus.Errorf("NewEvent missing field from in map filter - status: namespace(%t)", ok_namespace)
			return nil
		}

		timeStr, ok_time := v["time"].(string)
		if !ok_time {
			logrus.Errorf("NewEvent missing field from in map filter - status: time(%t)", ok_time)
			return nil
		}

		eventTime, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			logrus.Errorf("error parsing %s", timeStr)
			return nil
		}

		metrics, ok_metrics := v["metrics"]
		if !ok_metrics {
			logrus.Errorf("NewEvent missing field from in map filter - status: metrics(%t)", ok_metrics)
			return nil
		}

		return &Event{
			namespace: namespace,
			time:      eventTime,
			metrics:   metrics,
		}
	default:
		logrus.Errorf("NewEvent filter did not return a map: %+v", in)
		return nil
	}
}

package flow

import (
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Event struct {
	namespace string
	time      time.Time
	metrics   any
}

type Namespace struct {
	Name    string             `json:"namespace" yaml:"namespace"`
	Group   string             `json:"group" yaml:"group"`
	Service string             `json:"service" yaml:"service"`
	Metrics map[string]*Metric `json:"metrics" yaml:"metrics"`
}

func NewNamespace(buf []byte, granularity int64, cardinality int64) *Namespace {
	var namespace Namespace

	if err := yaml.Unmarshal(buf, &namespace); err != nil {
		logrus.Errorf("NewNamespace: %+v", err)
		return nil
	}

	for metricName, metric := range namespace.Metrics {
		metric.Name = metricName
		metric.Init(granularity, cardinality)
	}

	if !namespace.validateConfig() {
		logrus.Errorf("NewNamespace: not a valid config")
		return nil
	}

	return &namespace
}

func (namespace *Namespace) validateConfig() bool {
	if len(namespace.Name) == 0 {
		return false
	}

	if len(namespace.Group) == 0 {
		return false
	}

	if len(namespace.Service) == 0 {
		return false
	}

	return true
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

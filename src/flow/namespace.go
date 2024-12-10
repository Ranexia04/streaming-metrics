package flow

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"example.com/streaming-metrics/src/prom"
	"example.com/streaming-metrics/src/store"
)

type Event struct {
	namespace string
	time      string
	metrics   any
}

type Namespace struct {
	Name    string                  `json:"namespace" yaml:"namespace"`
	Group   string                  `json:"group" yaml:"group"`
	Service string                  `json:"service" yaml:"service"`
	Metrics map[string]*prom.Metric `json:"metrics" yaml:"metrics"`
	Store   *store.MemoryStore
}

func NewNamespace(buf []byte, granularity int64, cardinality int64) *Namespace {
	var namespace Namespace

	if err := yaml.Unmarshal(buf, &namespace); err != nil {
		logrus.Errorf("NewNamespace: %+v", err)
		return nil
	}

	for metricName, metric := range namespace.Metrics {
		metric.Name = metricName
		metric.AddPromMetric()
	}

	if !namespace.validateConfig() {
		logrus.Errorf("NewNamespace: not a valid config")
		return nil
	}

	namespace.Store = store.NewMemoryStore(namespace.Metrics, granularity, cardinality)
	if namespace.Store == nil {
		logrus.Errorf("NewNamespace %s: unable to create store", namespace.Name)
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

func eventFromAny(in any) *Event {
	switch v := in.(type) {
	case map[string]any:
		namespace, ok_namespace := v["namespace"].(string)
		time, ok_time := v["time"].(string)
		metrics, ok_metrics := v["metrics"]

		if !ok_namespace {
			logrus.Errorf("eventFromAny missing field from in map filter - status: namespace(%t)", ok_namespace)
			return nil
		}

		if !ok_time {
			logrus.Errorf("eventFromAny missing field from in map filter - status: time(%t)", ok_time)
			return nil
		}

		if !ok_metrics {
			logrus.Errorf("eventFromAny missing field from in map filter - status: metrics(%t)", ok_metrics)
			return nil
		}

		return &Event{
			namespace: namespace,
			time:      time,
			metrics:   metrics,
		}
	default:
		logrus.Errorf("eventFromAny filter did not return a map: %+v", in)
		return nil
	}

}

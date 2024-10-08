package flow

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Metric struct {
	namespace string
	time      string
	metric    any
}

type Namespace struct {
	Group     string `json:"group" yaml:"group"`
	Namespace string `json:"namespace" yaml:"namespace"`
}

/*
 * Namespace
 */

func NewNamespace(buf []byte) *Namespace {
	var namespace Namespace

	if err := yaml.Unmarshal(buf, &namespace); err != nil {
		logrus.Errorf("New_namespace: %+v", err)
		return nil
	}

	if !namespace.validateConfig() {
		logrus.Errorf("New_namespace: not a valid config")
		return nil
	}

	return &namespace
}

func (namespace *Namespace) validateConfig() bool {
	return len(namespace.Namespace) > 0
}

func metricFromAny(in any) *Metric {
	switch v := in.(type) {
	case map[string]any:
		namespace, ok_namespace := v["namespace"].(string)
		time, ok_time := v["time"].(string)
		metric, ok_metric := v["metric"]

		if !ok_namespace || !ok_time || !ok_metric {
			logrus.Errorf("metricFromAny missing field from in map filter - status: namespace(%t) time(%t) metric(%t)", ok_namespace, ok_time, ok_metric)
			return nil
		}

		return &Metric{
			namespace: namespace,
			time:      time,
			metric:    metric,
		}
	default:
		logrus.Errorf("metricFromAny filter did not return a map: %+v", in)
		return nil
	}

}

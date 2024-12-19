package flow

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Namespace struct {
	Name    string             `json:"namespace" yaml:"namespace"`
	Group   string             `json:"group" yaml:"group"`
	Service string             `json:"service" yaml:"service"`
	Metrics map[string]*Metric `json:"metrics" yaml:"metrics"`
}

func NewNamespace(buf []byte, granularity int64, cardinality int64, shift int64) *Namespace {
	var namespace Namespace

	if err := yaml.Unmarshal(buf, &namespace); err != nil {
		logrus.Errorf("NewNamespace: %+v", err)
		return nil
	}

	for metricName, metric := range namespace.Metrics {
		metric.Name = metricName
		metric.Init(granularity, cardinality, shift)
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

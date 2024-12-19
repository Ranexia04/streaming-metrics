package flow

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"example.com/streaming-metrics/src/prom"
	"example.com/streaming-metrics/src/store"
)

type Metric struct {
	Name    string
	Help    string    `yaml:"help"`
	Type    string    `yaml:"type"`
	Buckets []float64 `yaml:"buckets"`
	Manager *store.MetricManager
}

func (m *Metric) Init(granularity int64, cardinality int64, shift int64) {
	_, exists := metricManagers[m.Name]
	if !exists {
		promMetric := NewPromMetric(m)
		prom.Reg.MustRegister(promMetric)
		metricManagers[m.Name] = store.NewMetricManager(m.Type, promMetric, granularity, cardinality, shift)
	}

	m.Manager = metricManagers[m.Name]
}

func NewPromMetric(m *Metric) prometheus.Collector {
	extraLabels := []string{"delay", "service", "group", "namespace", "hostname"}

	var metric prometheus.Collector
	switch m.Type {
	case "counter":
		metric = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: m.Name,
				Help: m.Help,
			},
			extraLabels,
		)

	case "gauge":
		metric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: m.Name,
				Help: m.Help,
			},
			extraLabels,
		)

	case "histogram":
		metric = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    m.Name,
				Help:    m.Help,
				Buckets: m.Buckets,
			},
			extraLabels,
		)

	case "summary":
		metric = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: m.Name,
				Help: m.Help,
			},
			extraLabels,
		)

	default:
		logrus.Panicf("unsupported metric type: %s", m.Type)
	}

	return metric
}

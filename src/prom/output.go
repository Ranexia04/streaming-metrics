package prom

import (
	"example.com/streaming-metrics/src/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var PromMetrics = make(map[string]prometheus.Collector)
var WindowManagers = make(map[string]*store.WindowManager)

type Metric struct {
	Name    string
	Help    string    `yaml:"help"`
	Type    string    `yaml:"type"`
	Buckets []float64 `yaml:"buckets"`

	PromMetric    prometheus.Collector
	WindowManager *store.WindowManager
	Update        func(interface{}, prometheus.Labels)
}

func (m *Metric) Init(granularity int64, cardinality int64) {
	_, exists := PromMetrics[m.Name]
	if !exists {
		PromMetrics[m.Name] = m.createPromMetric()
	}

	m.PromMetric = PromMetrics[m.Name]

	m.setUpdateMethod()

	_, exists = WindowManagers[m.Name]
	if !exists {
		WindowManagers[m.Name] = store.NewWindowManager(m.Type, granularity, cardinality)
	}

	m.WindowManager = WindowManagers[m.Name]
}

func (m *Metric) createPromMetric() prometheus.Collector {
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

	reg.MustRegister(metric)

	return metric
}

func (m *Metric) setUpdateMethod() {
	switch m.Type {
	case "counter":
		m.Update = m.updateCounter

	case "gauge":

		m.Update = m.updateGauge

	case "histogram":

		m.Update = m.updateHistogram

	case "summary":

		m.Update = m.updateSummary

	default:
		logrus.Panicf("unsupported metric type: %s", m.Type)
	}
}

func (m *Metric) updateCounter(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", value)
		return
	}

	m.PromMetric.(*prometheus.CounterVec).With(extraLabels).Add(float64(metricValue))
}

func (m *Metric) updateGauge(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}

	m.PromMetric.(*prometheus.GaugeVec).With(extraLabels).Set(metricValue)
}

func (m *Metric) updateHistogram(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for histogram metric", metricValue)
		return
	}

	m.PromMetric.(*prometheus.HistogramVec).With(extraLabels).Observe(metricValue)
}

func (m *Metric) updateSummary(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for summary metric", metricValue)
		return
	}

	m.PromMetric.(*prometheus.SummaryVec).With(extraLabels).Observe(metricValue)
}

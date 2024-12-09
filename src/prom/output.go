package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type PromMetrics struct {
	CounterMetrics   map[string]*prometheus.CounterVec
	GaugeMetrics     map[string]*prometheus.GaugeVec
	HistogramMetrics map[string]*prometheus.HistogramVec
	SummaryMetrics   map[string]*prometheus.SummaryVec
}

var MyPromMetrics = &PromMetrics{
	CounterMetrics:   make(map[string]*prometheus.CounterVec),
	GaugeMetrics:     make(map[string]*prometheus.GaugeVec),
	HistogramMetrics: make(map[string]*prometheus.HistogramVec),
	SummaryMetrics:   make(map[string]*prometheus.SummaryVec),
}

type Metric struct {
	Name    string
	Help    string    `yaml:"help"`
	Type    string    `yaml:"type"`
	Buckets []float64 `yaml:"buckets"`

	PromMetric prometheus.Collector
	Update     func(interface{}, prometheus.Labels)
}

func (metric *Metric) AddPromMetric() {
	extraLabels := []string{"delay", "service", "group", "namespace", "hostname"}
	switch metric.Type {
	case "counter":
		counter, exists := MyPromMetrics.CounterMetrics[metric.Name]
		if !exists {
			counter = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				extraLabels,
			)
			reg.MustRegister(counter)
			MyPromMetrics.CounterMetrics[metric.Name] = counter
			logrus.Infof("registered %v counter metric", metric.Name)
		}

		metric.PromMetric = counter
		metric.Update = metric.updateCounter

	case "gauge":
		gauge, exists := MyPromMetrics.GaugeMetrics[metric.Name]
		if !exists {
			gauge = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				extraLabels,
			)
			reg.MustRegister(gauge)
			MyPromMetrics.GaugeMetrics[metric.Name] = gauge
			logrus.Infof("registered %v gauge metric", metric.Name)
		}

		metric.PromMetric = gauge
		metric.Update = metric.updateGauge

	case "histogram":
		histogram, exists := MyPromMetrics.HistogramMetrics[metric.Name]
		if !exists {
			histogram = prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    metric.Name,
					Help:    metric.Help,
					Buckets: metric.Buckets,
				},
				extraLabels,
			)
			reg.MustRegister(histogram)
			MyPromMetrics.HistogramMetrics[metric.Name] = histogram
			logrus.Infof("registered %v histogram metric", metric.Name)
		}

		metric.PromMetric = histogram
		metric.Update = metric.updateHistogram

	case "summary":
		summary, exists := MyPromMetrics.SummaryMetrics[metric.Name]
		if !exists {
			summary = prometheus.NewSummaryVec(
				prometheus.SummaryOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				extraLabels,
			)
			reg.MustRegister(summary)
			MyPromMetrics.SummaryMetrics[metric.Name] = summary
			logrus.Infof("registered %v summary metric", metric.Name)
		}

		metric.PromMetric = summary
		metric.Update = metric.updateSummary

	default:
		logrus.Panicf("unsupported metric type: %s", metric.Type)
	}
}

func (metric *Metric) updateCounter(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", value)
		return
	}

	metric.PromMetric.(*prometheus.CounterVec).With(extraLabels).Add(float64(metricValue))
}

func (metric *Metric) updateGauge(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}

	metric.PromMetric.(*prometheus.GaugeVec).With(extraLabels).Set(metricValue)
}

func (metric *Metric) updateHistogram(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for histogram metric", metricValue)
		return
	}

	metric.PromMetric.(*prometheus.HistogramVec).With(extraLabels).Observe(metricValue)
}

func (metric *Metric) updateSummary(value interface{}, extraLabels prometheus.Labels) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for summary metric", metricValue)
		return
	}

	metric.PromMetric.(*prometheus.SummaryVec).With(extraLabels).Observe(metricValue)
}

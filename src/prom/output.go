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
	Update     func(string, string, interface{})
}

func (metric *Metric) AddPromMetric() {
	switch metric.Type {
	case "counter":
		counter, exists := MyPromMetrics.CounterMetrics[metric.Name]
		if !exists {
			counter = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				[]string{"namespace", "hostname"},
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
				[]string{"namespace", "hostname"},
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
				[]string{"namespace", "hostname"},
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
				[]string{"namespace", "hostname"},
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

func (metric *Metric) updateCounter(namespaceName string, hostname string, value interface{}) {
	metricValue, ok := value.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", value)
		return
	}

	promLabels := prometheus.Labels{"namespace": namespaceName, "hostname": hostname}
	metric.PromMetric.(*prometheus.CounterVec).With(promLabels).Add(float64(metricValue))
}

func (metric *Metric) updateGauge(namespaceName string, hostname string, value interface{}) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}

	promLabels := prometheus.Labels{"namespace": namespaceName, "hostname": hostname}
	metric.PromMetric.(*prometheus.GaugeVec).With(promLabels).Set(metricValue)
}

func (metric *Metric) updateHistogram(namespaceName string, hostname string, value interface{}) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for histogram metric", metricValue)
		return
	}

	promLabels := prometheus.Labels{"namespace": namespaceName, "hostname": hostname}
	metric.PromMetric.(*prometheus.HistogramVec).With(promLabels).Observe(metricValue)
}

func (metric *Metric) updateSummary(namespaceName string, hostname string, value interface{}) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for summary metric", metricValue)
		return
	}

	promLabels := prometheus.Labels{"namespace": namespaceName, "hostname": hostname}
	metric.PromMetric.(*prometheus.SummaryVec).With(promLabels).Observe(metricValue)
}

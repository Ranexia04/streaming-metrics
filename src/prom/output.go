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
	Help    string    `yaml:"help"`
	Type    string    `yaml:"type"`
	Buckets []float64 `yaml:"buckets"`

	PromMetric prometheus.Collector
	Update     func(string, interface{})
}

func (metric *Metric) AddPromMetric(metricName string) {
	switch metric.Type {
	case "counter":
		var counter *prometheus.CounterVec
		if _, exists := MyPromMetrics.CounterMetrics[metricName]; !exists {
			counter = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: metricName,
					Help: metric.Help,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(counter)
			MyPromMetrics.CounterMetrics[metricName] = counter
			logrus.Infof("registered %v counter metric", metricName)
		}

		metric.PromMetric = counter
		metric.Update = metric.updateCounter

	case "gauge":
		var gauge *prometheus.GaugeVec
		if _, exists := MyPromMetrics.GaugeMetrics[metricName]; !exists {
			gauge = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: metricName,
					Help: metric.Help,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(gauge)
			MyPromMetrics.GaugeMetrics[metricName] = gauge
			logrus.Infof("registered %v gauge metric", metricName)
		}

		metric.PromMetric = gauge
		metric.Update = metric.updateGauge

	case "histogram":
		var histogram *prometheus.HistogramVec
		if _, exists := MyPromMetrics.HistogramMetrics[metricName]; !exists {
			histogram = prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    metricName,
					Help:    metric.Help,
					Buckets: metric.Buckets,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(histogram)
			MyPromMetrics.HistogramMetrics[metricName] = histogram
			logrus.Infof("registered %v histogram metric", metricName)
		}

		metric.PromMetric = histogram
		metric.Update = metric.updateHistogram

	case "summary":
		var summary *prometheus.SummaryVec
		if _, exists := MyPromMetrics.SummaryMetrics[metricName]; !exists {
			summary = prometheus.NewSummaryVec(
				prometheus.SummaryOpts{
					Name: metricName,
					Help: metric.Help,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(summary)
			MyPromMetrics.SummaryMetrics[metricName] = summary
			logrus.Infof("registered %v summary metric", metricName)
		}

		metric.PromMetric = summary
		metric.Update = metric.updateSummary

	default:
		logrus.Panicf("unsupported metric type: %s", metric.Type)
	}
}

func (metric *Metric) updateCounter(namespaceName string, _ interface{}) {
	metric.PromMetric.(*prometheus.CounterVec).With(prometheus.Labels{"namespace": namespaceName}).Inc()
}

func (metric *Metric) updateGauge(namespaceName string, value interface{}) {
	metric.PromMetric.(*prometheus.GaugeVec).With(prometheus.Labels{"namespace": namespaceName}).Set(value.(float64))
}

func (metric *Metric) updateHistogram(namespaceName string, value interface{}) {
	metric.PromMetric.(*prometheus.HistogramVec).With(prometheus.Labels{"namespace": namespaceName}).Observe(value.(float64))
}

func (metric *Metric) updateSummary(namespaceName string, value interface{}) {
	metric.PromMetric.(*prometheus.SummaryVec).With(prometheus.Labels{"namespace": namespaceName}).Observe(value.(float64))
}

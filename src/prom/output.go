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
	Update     func(string, interface{})
}

func (metric *Metric) AddPromMetric() {
	switch metric.Type {
	case "counter":
		var counter *prometheus.CounterVec
		if _, exists := MyPromMetrics.CounterMetrics[metric.Name]; !exists {
			counter = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(counter)
			MyPromMetrics.CounterMetrics[metric.Name] = counter
			logrus.Infof("registered %v counter metric", metric.Name)
		}

		metric.PromMetric = counter
		metric.Update = metric.updateCounter

	case "gauge":
		var gauge *prometheus.GaugeVec
		if _, exists := MyPromMetrics.GaugeMetrics[metric.Name]; !exists {
			gauge = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(gauge)
			MyPromMetrics.GaugeMetrics[metric.Name] = gauge
			logrus.Infof("registered %v gauge metric", metric.Name)
		}

		metric.PromMetric = gauge
		metric.Update = metric.updateGauge

	case "histogram":
		var histogram *prometheus.HistogramVec
		if _, exists := MyPromMetrics.HistogramMetrics[metric.Name]; !exists {
			histogram = prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    metric.Name,
					Help:    metric.Help,
					Buckets: metric.Buckets,
				},
				[]string{"namespace"},
			)
			reg.MustRegister(histogram)
			MyPromMetrics.HistogramMetrics[metric.Name] = histogram
			logrus.Infof("registered %v histogram metric", metric.Name)
		}

		metric.PromMetric = histogram
		metric.Update = metric.updateHistogram

	case "summary":
		var summary *prometheus.SummaryVec
		if _, exists := MyPromMetrics.SummaryMetrics[metric.Name]; !exists {
			summary = prometheus.NewSummaryVec(
				prometheus.SummaryOpts{
					Name: metric.Name,
					Help: metric.Help,
				},
				[]string{"namespace"},
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

func (metric *Metric) updateCounter(namespaceName string, value interface{}) {
	metricValue, ok := value.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", value)
		return
	}
	metric.PromMetric.(*prometheus.CounterVec).With(prometheus.Labels{"namespace": namespaceName}).Add(float64(metricValue))
}

func (metric *Metric) updateGauge(namespaceName string, value interface{}) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}
	metric.PromMetric.(*prometheus.GaugeVec).With(prometheus.Labels{"namespace": namespaceName}).Set(metricValue)
}

func (metric *Metric) updateHistogram(namespaceName string, value interface{}) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for histogram metric", metricValue)
		return
	}
	metric.PromMetric.(*prometheus.HistogramVec).With(prometheus.Labels{"namespace": namespaceName}).Observe(metricValue)
}

func (metric *Metric) updateSummary(namespaceName string, value interface{}) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for summary metric", metricValue)
		return
	}
	metric.PromMetric.(*prometheus.SummaryVec).With(prometheus.Labels{"namespace": namespaceName}).Observe(metricValue)
}

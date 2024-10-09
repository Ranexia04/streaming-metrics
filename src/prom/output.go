package prom

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type Metric struct {
	Help    string    `yaml:"help"`
	Type    string    `yaml:"type"`
	Buckets []float64 `yaml:"buckets"`

	PromMetric prometheus.Collector
}

func (m *Metric) NewPromMetric() {
	//
}

func (m *Metric) UpdateMetric(namespace string, value interface{}) error {
	switch metric := m.PromMetric.(type) {
	case *prometheus.CounterVec:
		metric.With(prometheus.Labels{"namespace": namespace}).Inc()

	case *prometheus.GaugeVec:
		metric.With(prometheus.Labels{"namespace": namespace}).Set(value.(float64))

	case *prometheus.HistogramVec:
		metric.With(prometheus.Labels{"namespace": namespace}).Observe(value.(float64))

	case *prometheus.SummaryVec:
		metric.With(prometheus.Labels{"namespace": namespace}).Observe(value.(float64))

	default:
		return fmt.Errorf("unsupported metric type")
	}
	return nil
}

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

func AddMetric(namespaceName string, metricName string, metric Metric) {
	switch metric.Type {
	case "counter":
		counter := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: metricName,
				Help: metric.Help,
			},
			[]string{"namespace"},
		)
		if _, exists := MyPromMetrics.CounterMetrics[metricName]; !exists {
			MyPromMetrics.CounterMetrics[metricName] = counter
			reg.MustRegister(counter)
			logrus.Infof("registered %v counter metric", metricName)
		}

	case "gauge":
		gauge := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metricName,
				Help: metric.Help,
			},
			[]string{"namespace"},
		)
		if _, exists := MyPromMetrics.GaugeMetrics[metricName]; !exists {
			MyPromMetrics.GaugeMetrics[metricName] = gauge
			reg.MustRegister(gauge)
			logrus.Infof("registered %v gauge metric", metricName)
		}

	case "histogram":
		histogram := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    metricName,
				Help:    metric.Help,
				Buckets: metric.Buckets,
			},
			[]string{"namespace"},
		)
		if _, exists := MyPromMetrics.HistogramMetrics[metricName]; !exists {
			MyPromMetrics.HistogramMetrics[metricName] = histogram
			reg.MustRegister(histogram)
			logrus.Infof("registered %v histogram metric", metricName)
		}

	case "summary":
		summary := prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: metricName,
				Help: metric.Help,
			},
			[]string{"namespace"},
		)
		if _, exists := MyPromMetrics.SummaryMetrics[metricName]; !exists {
			MyPromMetrics.SummaryMetrics[metricName] = summary
			reg.MustRegister(summary)
			logrus.Infof("registered %v summary metric", metricName)
		}

	default:
		logrus.Panicf("unsupported metric type: %s", metric.Type)
	}
}

package store

import (
	"container/list"
	"strings"
	"sync"
	"time"

	"example.com/streaming-metrics/src/prom"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var Granularity int64
var Cardinality int64
var Shift int64

type Metric struct {
	Name             string
	Help             string
	Type             string
	LabelNames       []string
	Buckets          []float64
	promMetric       prometheus.Collector
	UpdatePromMetric func(prometheus.Labels, any)

	windows map[string]*Window
	mutex   sync.Mutex
}

func NewMetric(name string, help string, metricType string, labelNames []string) *Metric {
	metric := &Metric{
		Name:       name,
		Help:       help,
		Type:       metricType,
		LabelNames: labelNames,
		windows:    make(map[string]*Window),
	}

	metric.promMetric = metric.NewPromMetric()
	prom.Reg.MustRegister(metric.promMetric)
	metric.setUpdateMethod(metricType)

	return metric
}

func (metric *Metric) NewPromMetric() prometheus.Collector {
	var promMetric prometheus.Collector
	switch metric.Type {
	case "counter":
		promMetric = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: metric.Name,
				Help: metric.Help,
			},
			metric.LabelNames,
		)

	case "gauge":
		promMetric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metric.Name,
				Help: metric.Help,
			},
			metric.LabelNames,
		)

	case "histogram":
		promMetric = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    metric.Name,
				Help:    metric.Help,
				Buckets: metric.Buckets,
			},
			metric.LabelNames,
		)

	case "summary":
		promMetric = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: metric.Name,
				Help: metric.Help,
			},
			metric.LabelNames,
		)

	default:
		logrus.Panicf("unsupported metric type: %s", metric.Type)
	}

	return promMetric
}

func (metric *Metric) setUpdateMethod(metricType string) {
	switch metricType {
	case "counter":
		metric.UpdatePromMetric = metric.updateCounter

	case "gauge":
		metric.UpdatePromMetric = metric.updateGauge

	case "histogram":
		metric.UpdatePromMetric = metric.updateHistogram

	case "summary":
		metric.UpdatePromMetric = metric.updateSummary

	default:
		logrus.Panicf("unsupported metric type: %s", metricType)
	}
}

func (metric *Metric) updateCounter(labels prometheus.Labels, value any) {
	metricValue, ok := value.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", value)
		return
	}

	metric.promMetric.(*prometheus.CounterVec).With(labels).Add(float64(metricValue))
}

func (metric *Metric) updateGauge(labels prometheus.Labels, value any) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}

	metric.promMetric.(*prometheus.GaugeVec).With(labels).Set(metricValue)
}

func (metric *Metric) updateHistogram(labels prometheus.Labels, value any) {
	v, ok := value.(*list.List)
	if !ok {
		logrus.Errorf("metric %v must be type *list.List for histogram metric", v)
		return
	}

	for e := v.Front(); e != nil; e = e.Next() {
		metricValue, ok := e.Value.(float64)
		if !ok {
			logrus.Errorf("list element %v must be type float64 for histogram metric", metricValue)
			continue
		}

		metric.promMetric.(*prometheus.HistogramVec).With(labels).Observe(metricValue)
	}
}

func (metric *Metric) updateSummary(labels prometheus.Labels, value any) {
	v, ok := value.(*list.List)
	if !ok {
		logrus.Errorf("metric %v must be type *list.List for summary metric", v)
		return
	}

	for e := v.Front(); e != nil; e = e.Next() {
		metricValue, ok := e.Value.(float64)
		if !ok {
			logrus.Errorf("list element %v must be type float64 for summary metric", metricValue)
			continue
		}

		metric.promMetric.(*prometheus.SummaryVec).With(labels).Observe(metricValue)
	}
}

func (metric *Metric) UpdateWindows(t time.Time, labels map[string]string, value any) {
	key := generateKey(labels)

	metric.mutex.Lock()
	if _, exists := metric.windows[key]; !exists {
		metric.windows[key] = newWindow(labels, metric.Type)
	}
	window := metric.windows[key]
	metric.mutex.Unlock()

	window.Update(t, value)
}

func (metric *Metric) Tick() {
	metric.mutex.Lock()
	defer metric.mutex.Unlock()

	for _, window := range metric.windows {
		oldestBucket := window.Roll()
		metric.UpdatePromMetric(window.labels, oldestBucket.data)
	}
}

func generateKey(labels map[string]string) string {
	var builder strings.Builder
	builder.Grow(64)
	builder.WriteString("delay=")
	builder.WriteString(labels["delay"])
	builder.WriteString(",tp=")
	builder.WriteString(labels["tp"])
	builder.WriteString(",systm=")
	builder.WriteString(labels["systm"])
	builder.WriteString(",dmn=")
	builder.WriteString(labels["dmn"])
	builder.WriteString(",cmpnnt=")
	builder.WriteString(labels["cmpnnt"])
	builder.WriteString(",nm=")
	builder.WriteString(labels["nm"])
	builder.WriteString(",oprtn=")
	builder.WriteString(labels["oprtn"])
	builder.WriteString(",hstnm=")
	builder.WriteString(labels["hstnm"])
	builder.WriteString(",isErr=")
	builder.WriteString(labels["isErr"])
	builder.WriteString(",sCd=")
	builder.WriteString(labels["sCd"])
	builder.WriteString(",nCd=")
	builder.WriteString(labels["nCd"])
	return builder.String()
}

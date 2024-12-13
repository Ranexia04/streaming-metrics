package store

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type MetricManager struct {
	metricType       string
	promMetric       prometheus.Collector
	UpdatePromMetric func(prometheus.Labels, any)

	windows     map[string]*Window
	mutex       sync.RWMutex
	cardinality int64
	granularity int64
}

func NewMetricManager(metricType string, promMetric prometheus.Collector, granularity int64, cardinality int64) *MetricManager {
	metricManager := &MetricManager{
		metricType:  metricType,
		promMetric:  promMetric,
		cardinality: cardinality,
		granularity: granularity,
		windows:     make(map[string]*Window),
	}

	metricManager.setUpdateMethod(metricType)

	return metricManager
}

func (mm *MetricManager) setUpdateMethod(metricType string) {
	switch metricType {
	case "counter":
		mm.UpdatePromMetric = mm.updateCounter

	case "gauge":
		mm.UpdatePromMetric = mm.updateGauge

	case "histogram":
		mm.UpdatePromMetric = mm.updateHistogram

	case "summary":
		mm.UpdatePromMetric = mm.updateSummary

	default:
		logrus.Panicf("unsupported metric type: %s", metricType)
	}
}

func (mm *MetricManager) updateCounter(extraLabels prometheus.Labels, value any) {
	metricValue, ok := value.(int)
	if !ok {
		logrus.Errorf("metric %v must be type int for counter metric", value)
		return
	}

	mm.promMetric.(*prometheus.CounterVec).With(extraLabels).Add(float64(metricValue))
}

func (mm *MetricManager) updateGauge(extraLabels prometheus.Labels, value any) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for gauge metric", metricValue)
		return
	}

	mm.promMetric.(*prometheus.GaugeVec).With(extraLabels).Set(metricValue)
}

func (mm *MetricManager) updateHistogram(extraLabels prometheus.Labels, value any) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for histogram metric", metricValue)
		return
	}

	mm.promMetric.(*prometheus.HistogramVec).With(extraLabels).Observe(metricValue)
}

func (mm *MetricManager) updateSummary(extraLabels prometheus.Labels, value any) {
	metricValue, ok := value.(float64)
	if !ok {
		logrus.Errorf("metric %v must be type float64 for summary metric", metricValue)
		return
	}

	mm.promMetric.(*prometheus.SummaryVec).With(extraLabels).Observe(metricValue)
}

func (mm *MetricManager) UpdateWindows(t time.Time, labels map[string]string, metric any) {
	key := generateKey(labels)

	mm.mutex.RLock()
	_, exists := mm.windows[key]
	mm.mutex.RUnlock()

	if !exists {
		mm.mutex.Lock()
		_, exists = mm.windows[key]
		if !exists {
			mm.windows[key] = newWindow(labels, mm.metricType, mm.granularity, mm.cardinality)
		}
		mm.mutex.Unlock()
	}

	mm.windows[key].Update(t, metric)
}

func (mm *MetricManager) Tick() {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	mm.tick()
}

func (mm *MetricManager) tick() {
	for _, window := range mm.windows {
		oldestBucket := window.getOldestBucket()
		mm.UpdatePromMetric(window.labels, oldestBucket.Data)
		window.Roll()
	}
}

func generateKey(labels map[string]string) string {
	var builder strings.Builder
	builder.Grow(64)
	builder.WriteString("delay=")
	builder.WriteString(labels["delay"])
	builder.WriteString(",service=")
	builder.WriteString(labels["service"])
	builder.WriteString(",group=")
	builder.WriteString(labels["group"])
	builder.WriteString(",namespace=")
	builder.WriteString(labels["namespace"])
	builder.WriteString(",hostname=")
	builder.WriteString(labels["hostname"])
	return builder.String()
}

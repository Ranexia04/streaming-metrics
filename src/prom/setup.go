package prom

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sirupsen/logrus"
)

type BasePromMetrics struct {
	namespaceCount     prometheus.Counter
	pulsarProcessedMsg prometheus.Counter
	filteredMsg        *prometheus.CounterVec
	filterTime         prometheus.Summary
	pushTime           prometheus.Summary
	processTime        prometheus.Summary

	SetNumberNamespaces     func(n int)
	IncProcessedMsg         func()
	ObserveProcessingTime   func(t time.Duration)
	ObserveFilterTime       func(t time.Duration)
	IncNamespaceFilteredMsg func(namespace string)
	ObservePushTime         func(t time.Duration)

	activateObserveProcessingTime bool
}

func (BasePromMetric *BasePromMetrics) register(reg *prometheus.Registry) {
	reg.MustRegister(BasePromMetric.namespaceCount)
	reg.MustRegister(BasePromMetric.pulsarProcessedMsg)
	reg.MustRegister(BasePromMetric.filteredMsg)

	if BasePromMetric.activateObserveProcessingTime {
		reg.MustRegister(BasePromMetric.filterTime)
		reg.MustRegister(BasePromMetric.pushTime)
		reg.MustRegister(BasePromMetric.processTime)
	}
}

func initBasePromMetrics(activateObserveProcessingTime bool) *BasePromMetrics {
	basePromMetric := &BasePromMetrics{
		activateObserveProcessingTime: activateObserveProcessingTime,

		namespaceCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "namespace_count",
				Help: "The total number of namespaces",
			},
		),
		pulsarProcessedMsg: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "pulsar_processed_msg",
				Help: "The total number of processed messages from pulsar.",
			},
		),
		filteredMsg: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "filtered_messages",
				Help: "The number of metrics generated per namespace",
			}, []string{"namespace"},
		),
		filterTime: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "filter_time",
				Help:       "The time to apply all filters to a message (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		pushTime: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "push_time",
				Help:       "The time to push a filtered message per namespace (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		processTime: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "process_time",
				Help:       "The time to process a message from pulsar (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
	}

	basePromMetric.SetNumberNamespaces = func(n int) {
		basePromMetric.namespaceCount.Add(float64(n))
	}

	basePromMetric.IncProcessedMsg = func() {
		basePromMetric.pulsarProcessedMsg.Inc()
	}

	basePromMetric.IncNamespaceFilteredMsg = func(namespace string) {
		basePromMetric.filteredMsg.With(prometheus.Labels{"namespace": namespace}).Inc()
	}

	if basePromMetric.activateObserveProcessingTime {
		basePromMetric.ObserveProcessingTime = func(t time.Duration) {
			go basePromMetric.processTime.Observe(float64(t / time.Microsecond))
		}
		basePromMetric.ObserveFilterTime = func(t time.Duration) {
			go basePromMetric.filterTime.Observe(float64(t / time.Microsecond))
		}
		basePromMetric.ObservePushTime = func(t time.Duration) {
			go basePromMetric.pushTime.Observe(float64(t / time.Microsecond))
		}
	} else {
		basePromMetric.ObserveProcessingTime = func(t time.Duration) {}
		basePromMetric.ObserveFilterTime = func(t time.Duration) {}
		basePromMetric.ObservePushTime = func(t time.Duration) {}
	}

	return basePromMetric
}

var BasePromMetric *BasePromMetrics

func SetupPrometheus(prometheusPort uint, activateObserveProcessingTime bool) {
	reg := prometheus.NewRegistry()

	BasePromMetric = initBasePromMetrics(activateObserveProcessingTime)
	BasePromMetric.register(reg)

	http.Handle(
		"/metrics",
		promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				Registry: reg,
			},
		),
	)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", prometheusPort), nil); err != nil {
		logrus.Panicf("error setting up prometheus: %+v", err)
	}
	logrus.Infof("metrics exposed at: localhost:%d/metrics", prometheusPort)
}

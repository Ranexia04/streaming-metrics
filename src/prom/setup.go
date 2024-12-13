package prom

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Reg *prometheus.Registry = prometheus.NewRegistry()

type BasePromMetrics struct {
	groupsGauge     prometheus.Gauge
	namespacesGauge prometheus.Gauge
	processedMsg    prometheus.Counter
	filteredMsg     *prometheus.CounterVec
	discardedMsg    *prometheus.CounterVec
	filterTime      prometheus.Summary
	pushTime        prometheus.Summary
	processTime     prometheus.Summary

	IncNumberGroups          func()
	SetNumberNamespaces      func(n int)
	IncProcessedMsg          func()
	IncNamespaceFilteredMsg  func(namespace string)
	IncNamespaceDiscardedMsg func(namespace string)
	ObserveProcessingTime    func(t time.Duration)
	ObserveFilterTime        func(t time.Duration)
	ObservePushTime          func(t time.Duration)
}

func initBasePromMetricsHandlers(activateObserveProcessingTime bool) {
	MyBasePromMetrics.IncNumberGroups = func() {
		MyBasePromMetrics.groupsGauge.Inc()
	}

	MyBasePromMetrics.SetNumberNamespaces = func(n int) {
		MyBasePromMetrics.namespacesGauge.Set(float64(n))
	}

	MyBasePromMetrics.IncProcessedMsg = func() {
		MyBasePromMetrics.processedMsg.Inc()
	}

	MyBasePromMetrics.IncNamespaceFilteredMsg = func(namespace string) {
		MyBasePromMetrics.filteredMsg.With(prometheus.Labels{"namespace": namespace}).Inc()
	}

	MyBasePromMetrics.IncNamespaceDiscardedMsg = func(namespace string) {
		MyBasePromMetrics.discardedMsg.With(prometheus.Labels{"namespace": namespace}).Inc()
	}

	if activateObserveProcessingTime {
		MyBasePromMetrics.ObserveProcessingTime = func(t time.Duration) {
			go MyBasePromMetrics.processTime.Observe(float64(t / time.Microsecond))
		}
		MyBasePromMetrics.ObserveFilterTime = func(t time.Duration) {
			go MyBasePromMetrics.filterTime.Observe(float64(t / time.Microsecond))
		}
		MyBasePromMetrics.ObservePushTime = func(t time.Duration) {
			go MyBasePromMetrics.pushTime.Observe(float64(t / time.Microsecond))
		}
	} else {
		MyBasePromMetrics.ObserveProcessingTime = func(t time.Duration) {}
		MyBasePromMetrics.ObserveFilterTime = func(t time.Duration) {}
		MyBasePromMetrics.ObservePushTime = func(t time.Duration) {}
	}
}

func registerBasePromMetrics(activateObserveProcessingTime bool) {
	Reg.MustRegister(MyBasePromMetrics.groupsGauge)
	Reg.MustRegister(MyBasePromMetrics.namespacesGauge)
	Reg.MustRegister(MyBasePromMetrics.processedMsg)
	Reg.MustRegister(MyBasePromMetrics.filteredMsg)
	Reg.MustRegister(MyBasePromMetrics.discardedMsg)

	if activateObserveProcessingTime {
		Reg.MustRegister(MyBasePromMetrics.filterTime)
		Reg.MustRegister(MyBasePromMetrics.pushTime)
		Reg.MustRegister(MyBasePromMetrics.processTime)
	}
}

var MyBasePromMetrics *BasePromMetrics = &BasePromMetrics{
	groupsGauge: prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "groups",
			Help: "The total number of groups",
		},
	),
	namespacesGauge: prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "namespaces",
			Help: "The total number of namespaces",
		},
	),
	processedMsg: prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processed_messages",
			Help: "The total number of processed messages from pulsar.",
		},
	),
	filteredMsg: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "filtered_messages",
			Help: "The number of metrics generated per namespace",
		}, []string{"namespace"},
	),
	discardedMsg: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "discarded_messages",
			Help: "The number of metrics discarded per namespace",
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

func SetupPrometheus(activateObserveProcessingTime bool) {
	initBasePromMetricsHandlers(activateObserveProcessingTime)
	registerBasePromMetrics(activateObserveProcessingTime)

	http.Handle(
		"/metrics",
		promhttp.HandlerFor(
			Reg,
			promhttp.HandlerOpts{
				Registry: Reg,
			},
		),
	)
}

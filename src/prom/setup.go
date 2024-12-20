package prom

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Reg *prometheus.Registry = prometheus.NewRegistry()

	processedMsg = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "processed_messages",
			Help: "The total number of processed messages from pulsar.",
		},
	)

	discardedMsg = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "discarded_messages",
			Help: "The number of metrics discarded per namespace",
		},
	)

	filterTime = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       "filter_time",
			Help:       "The time to apply all filters to a message (µs)",
			Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
		},
	)

	pushTime = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       "push_time",
			Help:       "The time to push a filtered message per namespace (µs)",
			Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
		},
	)

	processTime = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       "process_time",
			Help:       "The time to process a message from pulsar (µs)",
			Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
		},
	)
)

func IncProcessedMsg() {
	processedMsg.Inc()
}

func IncDiscardedMsg() {
	discardedMsg.Inc()
}

var (
	ObserveProcessingTime func(t time.Duration)
	ObserveFilterTime     func(t time.Duration)
	ObservePushTime       func(t time.Duration)
)

func initBasePromMetricsHandlers(activateObserveProcessingTime bool) {
	if activateObserveProcessingTime {
		ObserveProcessingTime = func(t time.Duration) {
			go processTime.Observe(float64(t / time.Microsecond))
		}
		ObserveFilterTime = func(t time.Duration) {
			go filterTime.Observe(float64(t / time.Microsecond))
		}
		ObservePushTime = func(t time.Duration) {
			go pushTime.Observe(float64(t / time.Microsecond))
		}
	} else {
		ObserveProcessingTime = func(t time.Duration) {}
		ObserveFilterTime = func(t time.Duration) {}
		ObservePushTime = func(t time.Duration) {}
	}
}

func registerBasePromMetrics(activateObserveProcessingTime bool) {
	Reg.MustRegister(processedMsg)
	Reg.MustRegister(discardedMsg)

	if activateObserveProcessingTime {
		Reg.MustRegister(filterTime)
		Reg.MustRegister(pushTime)
		Reg.MustRegister(processTime)
	}
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

package prom_metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sirupsen/logrus"
)

type BasePromMetrics struct {
	namespace_count           prometheus.Counter
	pulsar_processed_msg      prometheus.Counter
	pulsar_processed_msg_time prometheus.Summary
	filter_time               prometheus.Summary
	filtered_msg              *prometheus.CounterVec
	// number_of_metrics_per_msg prometheus.Summary
	push_time prometheus.Summary

	Number_of_namespaces              func(n int)
	Inc_number_processed_msg          func()
	Observe_processing_time           func(t time.Duration)
	Observe_filter_time               func(t time.Duration)
	Inc_namespace_number_filtered_msg func(namespace string)
	Observe_push_time                 func(t time.Duration)
	Inc_monitors_ticks                func(namespace string)
	Inc_monitors_sent                 func(namespace string, pulsar_event string)

	activate_observe_processing_time bool
}

func (BasePromMetric *BasePromMetrics) register(reg *prometheus.Registry) {
	reg.MustRegister(BasePromMetric.namespace_count)
	reg.MustRegister(BasePromMetric.pulsar_processed_msg)
	reg.MustRegister(BasePromMetric.filtered_msg)

	if BasePromMetric.activate_observe_processing_time {
		reg.MustRegister(BasePromMetric.filter_time)
		reg.MustRegister(BasePromMetric.push_time)
		reg.MustRegister(BasePromMetric.pulsar_processed_msg_time)
	}
}

func initBasePromMetrics(activate_observe_processing_time bool) *BasePromMetrics {
	basePromMetric := &BasePromMetrics{
		activate_observe_processing_time: activate_observe_processing_time,

		namespace_count: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "namespace_count",
				Help: "The total number of namespaces",
			},
		),
		pulsar_processed_msg: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "pulsar_processed_msg",
				Help: "The total number of processed messages from pulsar.",
			},
		),
		pulsar_processed_msg_time: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "pulsar_processed_msg_time",
				Help:       "The time to process a message from pulsar (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		filter_time: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "filter_time",
				Help:       "The time to apply all filters to a message (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		filtered_msg: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "filtered_messages",
				Help: "The number of metrics generated per namespace",
			}, []string{"namespace"},
		),
		push_time: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "push_time",
				Help:       "The time to push a filtered message per namespace (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
	}

	basePromMetric.Number_of_namespaces = func(n int) {
		basePromMetric.namespace_count.Add(float64(n))
	}

	basePromMetric.Inc_number_processed_msg = func() {
		basePromMetric.pulsar_processed_msg.Inc()
	}

	basePromMetric.Inc_namespace_number_filtered_msg = func(namespace string) {
		basePromMetric.filtered_msg.With(prometheus.Labels{"namespace": namespace}).Inc()
	}

	if basePromMetric.activate_observe_processing_time {
		basePromMetric.Observe_processing_time = func(t time.Duration) {
			go basePromMetric.pulsar_processed_msg_time.Observe(float64(t / time.Microsecond))
		}
		basePromMetric.Observe_filter_time = func(t time.Duration) {
			go basePromMetric.filter_time.Observe(float64(t / time.Microsecond))
		}
		basePromMetric.Observe_push_time = func(t time.Duration) {
			go basePromMetric.push_time.Observe(float64(t / time.Microsecond))
		}
	} else {
		basePromMetric.Observe_processing_time = func(t time.Duration) {}
		basePromMetric.Observe_filter_time = func(t time.Duration) {}
		basePromMetric.Observe_push_time = func(t time.Duration) {}
	}

	return basePromMetric
}

var BasePromMetric *BasePromMetrics

func SetupPrometheus(prometheusport uint, activate_observe_processing_time bool) {
	reg := prometheus.NewRegistry()

	BasePromMetric = initBasePromMetrics(activate_observe_processing_time)
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

	if err := http.ListenAndServe(fmt.Sprintf(":%d", prometheusport), nil); err != nil {
		logrus.Panicf("error setting up prometheus: %+v", err)
	}
	logrus.Infof("metrics exposed at: localhost:%d/metrics", prometheusport)
}

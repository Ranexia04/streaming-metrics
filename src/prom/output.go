package prom

import (
	"fmt"

	"example.com/streaming-metrics/src/flow"
	"github.com/prometheus/client_golang/prometheus"
)

func CreateMetrics(metricsConfig map[string]*flow.Namespace) error {
	for name, metric := range metricsConfig {
		var promMetric prometheus.Collector

		switch metric.Type {
		case "counter":
			promMetric = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: name, // Use the map key as the metric name
					Help: metric.Help,
				},
				metric.Labels,
			)

		case "gauge":
			promMetric = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: name, // Use the map key as the metric name
					Help: metric.Help,
				},
				metric.Labels,
			)

		case "histogram":
			promMetric = prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    name, // Use the map key as the metric name
					Help:    metric.Help,
					Buckets: metric.Buckets, // Buckets for histograms
				},
				metric.Labels,
			)

		default:
			return fmt.Errorf("unsupported metric type: %s", metric.Type)
		}

		// Register the created metric with Prometheus
		err := prometheus.Register(promMetric)
		if err != nil {
			return fmt.Errorf("failed to register metric %s: %v", name, err)
		}
	}
	return nil
}

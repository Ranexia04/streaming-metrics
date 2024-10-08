package flow

import (
	"encoding/json"
	"time"

	"example.com/streaming-metrics/src/prom"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func Consumer(consume_chan <-chan pulsar.ConsumerMessage, ack_chan chan<- pulsar.ConsumerMessage, namespaces map[string]*Namespace, filters *FilterRoot, tick <-chan time.Time) {
	var nRead float64 = 0

	lastInstant := time.Now()
	lastPublishTime := time.Unix(0, 0)
	log_tick := time.NewTicker(time.Minute)
	defer log_tick.Stop()

	for {
		select {
		case msg := <-consume_chan:
			nRead += 1
			lastPublishTime = msg.PublishTime()

			consumeStart := time.Now()

			metrics := filter(msg.Payload(), filters)

			filterDur := time.Since(consumeStart)

			push_start := time.Now()
			for i := 0; i < len(metrics); i++ {
				metric := &metrics[i]
				prom.BasePromMetric.IncNamespaceFilteredMsg(metric.namespace)
				if namespace, ok := namespaces[metric.namespace]; ok {
					logrus.Printf("%v", namespace.Namespace)
					// add inc logic
				} else {
					logrus.Errorf("No namespace named: %s", metric.namespace)
				}
			}
			pushDur := time.Since(push_start)
			prom.BasePromMetric.ObservePushTime(pushDur)
			ack_chan <- msg

			processDur := time.Since(consumeStart)
			prom.BasePromMetric.ObserveFilterTime(filterDur)
			prom.BasePromMetric.ObserveProcessingTime(processDur)

		case <-log_tick.C:
			since := time.Since(lastInstant)
			lastInstant = time.Now()
			logrus.Infof("Read rate: %.3f msg/s; (last pulsar time %v)", nRead/float64(since/time.Second), lastPublishTime)
			nRead = 0
		}
	}
}

func filter(msg []byte, filters *FilterRoot) []Metric {
	filtered := make([]Metric, 0)

	var msg_json any

	if err := json.Unmarshal(msg, &msg_json); err != nil {
		logrus.Errorf("filter unmarshal msg: %+v", err)
		return filtered
	}

	// filter_start := time.Now()

	iter := filters.group_filter.Run(msg_json)

	v, ok := iter.Next()
	if !ok {
		return filtered
	}
	if _, ok := v.(error); ok {
		// ignore -- msg is not important for this namespace
		logrus.Tracef("filter group_filter next err: %+v", v.(error))
		return filtered
	}

	var group_name string

	switch gn := v.(type) {
	case string:
		group_name = gn

	default:
		logrus.Errorf("filter_root did not return string: %+v", v)
		return filtered
	}

	group_filters, ok := filters.groups[group_name]
	if !ok {
		logrus.Errorf("filter_root group does not exist: %s", group_name)
		return filtered
	}

	for _, filter := range group_filters.children {
		iter := filter.Filter.Run(msg_json)

		v, ok := iter.Next()
		if !ok {
			continue
		}
		if _, ok := v.(error); ok {
			// ignore -- msg is not important for this namespace
			logrus.Tracef("filter next err: %+v", v.(error))
			continue
		} else {
			metric := metricFromAny(v)

			if metric != nil {
				filtered = append(filtered, *metric)
			}
		}
	}

	// go prom.Filter_time.Observe(float64(time.Since(filter_start) / time.Microsecond))
	// go prom.Number_of_metrics_per_msg.Observe(float64(len(filtered)))

	return filtered
}

func Acknowledger(consumer pulsar.Consumer, ack_chan <-chan pulsar.ConsumerMessage) {
	last_instant := time.Now()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	var ack float64 = 0
	for {
		select {
		case msg := <-ack_chan:

			if err := consumer.Ack(msg); err != nil {
				logrus.Warnf("consumer.Acks err: %+v", err)
			}
			ack++

			prom.BasePromMetric.IncProcessedMsg()

		case <-tick.C:
			since := time.Since(last_instant)
			last_instant = time.Now()
			logrus.Infof("Ack rate: %.3f msg/s", ack/float64(since/time.Second))
			ack = 0
		}
	}
}

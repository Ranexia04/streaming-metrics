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

			filteredEvents := filter(msg.Payload(), filters)

			filterDur := time.Since(consumeStart)

			push_start := time.Now()
			for i := 0; i < len(filteredEvents); i++ {
				metric := &filteredEvents[i]
				prom.BasePromMetric.IncNamespaceFilteredMsg(metric.namespace)
				if namespace, ok := namespaces[metric.namespace]; ok {
					logrus.Printf("%v", namespace.Name)
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

func filter(msg []byte, filters *FilterRoot) []Event {
	filteredEvents := make([]Event, 0)

	var msgJson any

	if err := json.Unmarshal(msg, &msgJson); err != nil {
		logrus.Errorf("filter unmarshal msg: %+v", err)
		return filteredEvents
	}

	iter := filters.groupFilter.Run(msgJson)

	v, ok := iter.Next()
	if !ok {
		return filteredEvents
	}
	if _, ok := v.(error); ok {
		// ignore -- msg is not important for this namespace
		logrus.Tracef("group_filter next err: %+v", v.(error))
		return filteredEvents
	}

	var groupName string

	switch gn := v.(type) {
	case string:
		groupName = gn

	default:
		logrus.Errorf("filter_root did not return string: %+v", v)
		return filteredEvents
	}

	groupFilters, ok := filters.groups[groupName]
	if !ok {
		logrus.Errorf("filter_root group does not exist: %s", groupName)
		return filteredEvents
	}

	for _, filter := range groupFilters.children {
		iter := filter.Filter.Run(msgJson)

		v, ok := iter.Next()
		if !ok {
			continue
		}
		if _, ok := v.(error); ok {
			// ignore -- msg is not important for this namespace
			logrus.Tracef("filter next err: %+v", v.(error))
			continue
		} else {
			event := eventFromAny(v)

			if event != nil {
				filteredEvents = append(filteredEvents, *event)
			}
		}
	}

	// go prom.Filter_time.Observe(float64(time.Since(filter_start) / time.Microsecond))
	// go prom.Number_of_metrics_per_msg.Observe(float64(len(filtered)))

	return filteredEvents
}

func Acknowledger(consumer pulsar.Consumer, ack_chan <-chan pulsar.ConsumerMessage) {
	lastInstant := time.Now()
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
			since := time.Since(lastInstant)
			lastInstant = time.Now()
			logrus.Infof("Ack rate: %.3f msg/s", ack/float64(since/time.Second))
			ack = 0
		}
	}
}

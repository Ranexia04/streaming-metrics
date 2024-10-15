package flow

import (
	"encoding/json"
	"time"

	"example.com/streaming-metrics/src/prom"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func Consumer(consumeChan <-chan pulsar.ConsumerMessage, ackChan chan<- pulsar.ConsumerMessage, namespaces map[string]*Namespace, filterRoot *FilterRoot) {
	var nRead float64 = 0

	lastInstant := time.Now()
	lastPublishTime := time.Unix(0, 0)
	log_tick := time.NewTicker(time.Minute)
	defer log_tick.Stop()

	for {
		select {
		case msg := <-consumeChan:
			nRead += 1
			lastPublishTime = msg.PublishTime()

			consumeStart := time.Now()

			events := filterEvents(msg.Payload(), filterRoot)

			filterDur := time.Since(consumeStart)

			push_start := time.Now()
			for _, event := range events {
				prom.MyBasePromMetrics.IncNamespaceFilteredMsg(event.namespace)

				namespace, ok := namespaces[event.namespace]
				if !ok {
					logrus.Errorf("No namespace named: %s", event.namespace)
				}
				updateMetrics(*namespace, event)
			}
			pushDur := time.Since(push_start)
			prom.MyBasePromMetrics.ObservePushTime(pushDur)
			ackChan <- msg

			processDur := time.Since(consumeStart)
			prom.MyBasePromMetrics.ObserveFilterTime(filterDur)
			prom.MyBasePromMetrics.ObserveProcessingTime(processDur)

		case <-log_tick.C:
			since := time.Since(lastInstant)
			lastInstant = time.Now()
			logrus.Infof("Read rate: %.3f msg/s; (last pulsar time %v)", nRead/float64(since/time.Second), lastPublishTime)
			nRead = 0
		}
	}
}

func filterEvents(msg []byte, filterRoot *FilterRoot) []Event {
	filteredEvents := make([]Event, 0)

	var msgJson any

	if err := json.Unmarshal(msg, &msgJson); err != nil {
		logrus.Errorf("filter unmarshal msg: %+v", err)
		return filteredEvents
	}

	iter := filterRoot.groupFilter.Run(msgJson)

	v, ok := iter.Next()
	if !ok {
		return filteredEvents
	}

	var results []interface{}
	switch r := v.(type) {
	case []interface{}:
		results = r
	default:
		logrus.Errorf("Unknown type: %T", r)
	}
	logrus.Tracef("groups matched: %+v", results)

	for _, result := range results {
		var groupName string
		switch gn := result.(type) {
		case string:
			groupName = gn
		default:
			logrus.Errorf("Unknown type for element: %T", gn)
		}

		groupFilters, ok := filterRoot.groups[groupName]
		if !ok {
			logrus.Errorf("filter_root group does not exist: %s", groupName)
			continue
		}

		for _, filter := range groupFilters.children {
			event := filterEventByNamespace(filter, msgJson)
			if event == nil {
				continue
			}

			filteredEvents = append(filteredEvents, *event)
		}
	}

	return filteredEvents
}

func filterEventByNamespace(filter *LeafNode, msgJson any) *Event {
	iter := filter.Filter.Run(msgJson)

	v, ok := iter.Next()
	if !ok {
		return nil
	}
	if _, ok := v.(error); ok {
		// ignore -- msg is not important for this namespace
		logrus.Tracef("filter next err: %+v", v.(error))
		return nil
	}

	event := eventFromAny(v)
	return event
}

func updateMetrics(namespace Namespace, event Event) {
	for eventMetricName, eventMetric := range event.metrics.(map[string]interface{}) {
		metric, exists := namespace.Metrics[eventMetricName]
		if !exists {
			logrus.Errorf("updateMetrics prometheus metric %v not found", eventMetricName)
			continue
		}

		metric.Update(namespace.Name, eventMetric)
	}
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

			prom.MyBasePromMetrics.IncProcessedMsg()

		case <-tick.C:
			since := time.Since(lastInstant)
			lastInstant = time.Now()
			logrus.Infof("Ack rate: %.3f msg/s", ack/float64(since/time.Second))
			ack = 0
		}
	}
}

package flow

import (
	"encoding/json"
	"time"

	"example.com/streaming-metrics/src/prom"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

var DelayLabel string

func Consumer(consumeChan <-chan pulsar.ConsumerMessage, ackChan chan<- pulsar.ConsumerMessage, namespaces map[string]*Namespace, filterRoot *FilterRoot) {
	var nRead float64 = 0

	lastInstant := time.Now()
	lastPublishTime := time.Unix(0, 0)
	logTick := time.NewTicker(time.Minute)
	defer logTick.Stop()

	for {
		select {
		case msg := <-consumeChan:
			nRead += 1
			lastPublishTime = msg.PublishTime()
			consumeStart := time.Now()

			var msgJson map[string]any
			if err := json.Unmarshal(msg.Payload(), &msgJson); err != nil {
				logrus.Errorf("filter unmarshal msg: %+v", err)
				ackChan <- msg
				continue
			}

			hostnameAny, ok := msgJson["hstnm"]
			if !ok {
				logrus.Error("No hostname found")
				ackChan <- msg
				continue
			}

			hostname, ok := hostnameAny.(string)
			if !ok {
				logrus.Error("Hostname is not a string")
				ackChan <- msg
				continue
			}

			events := filterEvents(msgJson, filterRoot)

			filterDur := time.Since(consumeStart)
			prom.ObserveFilterTime(filterDur)

			pushStart := time.Now()
			for _, event := range events {
				prom.IncNamespaceFilteredMsg(event.namespace)

				namespace, ok := namespaces[event.namespace]
				if !ok {
					logrus.Errorf("No namespace named: %s", event.namespace)
					continue
				}

				updateMetrics(*namespace, hostname, event)
			}

			pushDur := time.Since(pushStart)
			prom.ObservePushTime(pushDur)

			processDur := time.Since(consumeStart)
			prom.ObserveProcessingTime(processDur)

			ackChan <- msg

		case <-logTick.C:
			since := time.Since(lastInstant)
			lastInstant = time.Now()
			logrus.Infof("Read rate: %.3f msg/s; (last pulsar time %v)", nRead/float64(since/time.Second), lastPublishTime)
			nRead = 0
		}
	}
}

func filterEvents(msgJson map[string]any, filterRoot *FilterRoot) []Event {
	filteredEvents := make([]Event, 0)

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

	event := NewEvent(v)
	return event
}

func updateMetrics(namespace Namespace, hostname string, event Event) {
	extraLabels := map[string]string{
		"delay":     "none",
		"service":   namespace.Service,
		"group":     namespace.Group,
		"namespace": namespace.Name,
		"hostname":  hostname,
	}

	for eventMetricName, eventMetric := range event.metrics.(map[string]any) {
		metric, exists := namespace.Metrics[eventMetricName]
		if !exists {
			logrus.Errorf("updateMetrics prometheus metric %v not found", eventMetricName)
			continue
		}

		extraLabels["delay"] = "none"
		metric.Manager.UpdatePromMetric(extraLabels, eventMetric)

		extraLabels["delay"] = DelayLabel
		metric.Manager.UpdateWindows(event.time, extraLabels, eventMetric)
	}
}

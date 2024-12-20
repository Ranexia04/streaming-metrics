package flow

import (
	"fmt"
	"time"

	"example.com/streaming-metrics/src/prom"
	"example.com/streaming-metrics/src/store"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

var DelayLabel string

var (
	labelNames      = []string{"delay", "tp", "systm", "dmn", "cmpnnt", "nm", "oprtn", "hstnm", "isErr", "sCd", "nCd"}
	requestCount    = store.NewMetric("request_count", "Counter of request received", "counter", labelNames)
	requestDuration = store.NewMetric("request_duration", "Duration of requests received in seconds", "histogram", labelNames)
)

func Consumer(consumeChan <-chan pulsar.ConsumerMessage, ackChan chan<- pulsar.ConsumerMessage) {
	var nRead float64 = 0
	lastPublishTime := time.Unix(0, 0)
	lastInstant := time.Now()
	logTick := time.NewTicker(time.Minute)
	defer logTick.Stop()

	for {
		select {
		case msg := <-consumeChan:
			nRead += 1
			lastPublishTime = msg.PublishTime()
			processMsg(msg, ackChan)

		case <-logTick.C:
			since := time.Since(lastInstant)
			lastInstant = time.Now()
			logrus.Infof("Read rate: %.3f msg/s; (last pulsar time %v)", nRead/float64(since/time.Second), lastPublishTime)
			nRead = 0
		}
	}
}

func processMsg(msg pulsar.ConsumerMessage, ackChan chan<- pulsar.ConsumerMessage) {
	defer acknowledgeMsg(msg, ackChan)

	consumeStart := time.Now()

	event := NewEvent(msg.Payload())

	if event == nil {
		return
	}

	filterDur := time.Since(consumeStart)
	prom.ObserveFilterTime(filterDur)

	pushStart := time.Now()
	updateMetrics(event)
	pushDur := time.Since(pushStart)
	prom.ObservePushTime(pushDur)

	processDur := time.Since(consumeStart)
	prom.ObserveProcessingTime(processDur)
}

func acknowledgeMsg(msg pulsar.ConsumerMessage, ackChan chan<- pulsar.ConsumerMessage) {
	ackChan <- msg
}

func updateMetrics(event *Event) {
	labels := map[string]string{
		"delay":  DelayLabel,
		"systm":  event.System,
		"tp":     event.Type,
		"dmn":    event.Domain,
		"cmpnnt": event.Component,
		"nm":     event.Function,
		"oprtn":  event.Operation,
		"hstnm":  event.Hostname,
		"isErr":  fmt.Sprintf("%t", event.IsError),
		"sCd":    event.StatusCode,
		"nCd":    event.NativeCode,
	}

	requestCount.UpdateWindows(event.Time, labels, 1)
	requestDuration.UpdateWindows(event.Time, labels, event.Duration/1000)
}

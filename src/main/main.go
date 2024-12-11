package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsar_log "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"

	"example.com/streaming-metrics/src/flow"
	"example.com/streaming-metrics/src/prom"
)

func setupLogging(level string) {
	logrus.SetFormatter(&logrus.JSONFormatter{
		//FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
	})
	l, err := logrus.ParseLevel(level)
	if err != nil {
		logrus.Errorf("Failed parse log level. Reason: %+v", err)
	} else {
		logrus.SetLevel(l)
	}
}

func newClient(url string, trust_cert_file string, cert_file string, key_file string, allow_insecure_connection bool) pulsar.Client {
	var client pulsar.Client
	var err error
	var auth pulsar.Authentication

	if len(cert_file) > 0 || len(key_file) > 0 {
		auth = pulsar.NewAuthenticationTLS(cert_file, key_file)
	}

	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
	})

	client, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:                        url,
		TLSAllowInsecureConnection: allow_insecure_connection,
		Authentication:             auth,
		TLSTrustCertsFilePath:      trust_cert_file,
		Logger:                     pulsar_log.NewLoggerWithLogrus(log),
	})

	if err != nil {
		logrus.Errorf("Failed connect to pulsar. Reason: %+v", err)
	}
	return client
}

var isReady atomic.Value

func readinessHandler(w http.ResponseWriter, r *http.Request) {
	var logMessage string
	if isReady.Load() == true {
		w.WriteHeader(http.StatusOK)
		logMessage = "app is ready\n"
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		logMessage = "app is not ready\n"
	}

	fmt.Fprint(w, logMessage)
}

func setupReadiness() {
	http.HandleFunc("/ready", readinessHandler)
}

func startHttp(httpPort uint) {
	logrus.Infof("exposing metrics at: localhost:%d/metrics", httpPort)
	logrus.Infof("exposing readiness at: localhost:%d/ready", httpPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
		logrus.Panicf("error setting up http server: %+v", err)
	}
}

func main() {
	isReady.Store(false)

	opt := loadArgs()

	setupLogging(opt.logLevel)
	logrus.Infof("%+v", opt)

	prom.SetupPrometheus(opt.activateObserveProcessingTime)
	setupReadiness()
	go startHttp(opt.httpPort)

	// Clients
	sourceClient := newClient(opt.pulsarUrl, opt.pulsarTrustCertsFile, opt.pulsarCertFile, opt.pulsarKeyFile, opt.pulsarAllowInsecureConnection)

	defer sourceClient.Close()

	consumeChan := make(chan pulsar.ConsumerMessage, 2000)
	ackChan := make(chan pulsar.ConsumerMessage, 2000)

	consumer, err := sourceClient.Subscribe(
		pulsar.ConsumerOptions{
			Topics:                      strings.Split(opt.pulsarTopic, ";"),
			SubscriptionName:            opt.pulsarSubscription,
			Name:                        opt.pulsarConsumer,
			Type:                        pulsar.Shared,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
			MessageChannel:              consumeChan,
			ReceiverQueueSize:           2000,
		},
	)
	if err != nil {
		logrus.Fatalln("Failed create consumer. Reason: ", err)
	}

	defer consumer.Close()

	logrus.Infoln("loading namespaces")
	namespaces := loadNamespaces(opt.namespacesDir, opt.Granularity, opt.Cardinality)
	logrus.Infoln("loading filters")
	filterRoot := loadFilters(opt.filtersDir, opt.groupsDir, namespaces)

	prom.MyBasePromMetrics.SetNumberNamespaces(len(namespaces))

	// Logic
	logrus.Infoln("starting consumer threads")
	for i := 0; i < int(opt.consumerThreads); i++ {
		go flow.Consumer(consumeChan, ackChan, namespaces, filterRoot)
	}
	go flow.Ticker(opt.Cardinality, namespaces)

	if opt.pprofOn {
		logrus.Infoln("starting profiler thread")
		go activateProfiling(opt.pprofDir, time.Duration(opt.pprofDuration)*time.Second)
	}

	isReady.Store(true)
	flow.Acknowledger(consumer, ackChan)
}

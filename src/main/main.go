package main

import (
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsar_log "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"

	"example.com/streaming_monitors/src/flow"
	"example.com/streaming_monitors/src/prom_metrics"
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

func main() {
	opt := loadArgs()

	setupLogging(opt.loglevel)
	logrus.Infof("%+v", opt)

	go prom_metrics.SetupPrometheus(opt.prometheusport, opt.activate_observe_processing_time)

	// Clients
	sourceClient := newClient(opt.sourcepulsar, opt.sourcetrustcerts, opt.sourcecertfile, opt.sourcekeyfile, opt.sourceallowinsecureconnection)
	destClient := newClient(opt.destpulsar, opt.desttrustcerts, opt.destcertfile, opt.destkeyfile, opt.destallowinsecureconnection)

	defer sourceClient.Close()
	defer destClient.Close()

	consume_chan := make(chan pulsar.ConsumerMessage, 2000)
	ack_chan := make(chan pulsar.ConsumerMessage, 2000)

	consumer, err := sourceClient.Subscribe(pulsar.ConsumerOptions{
		Topics:                      strings.Split(opt.sourcetopic, ";"),
		SubscriptionName:            opt.sourcesubscription,
		Name:                        opt.sourcename,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		MessageChannel:              consume_chan,
		ReceiverQueueSize:           2000,
	})
	if err != nil {
		logrus.Fatalln("Failed create consumer. Reason: ", err)
	}

	defer consumer.Close()

	configs := loadConfigs(opt.monitorsdir)
	namespaces := loadNamespaces(opt.monitorsdir, configs)
	filters := loadFilters(opt.monitorsdir, configs)

	prom_metrics.BasePromMetric.Number_of_namespaces(len(namespaces))

	// Logic
	tick := time.NewTicker(time.Second * time.Duration(opt.tickerseconds))
	for i := 0; i < int(opt.consumerthreads); i++ {
		go flow.Consumer(consume_chan, ack_chan, namespaces, filters, tick.C)
	}

	if opt.pprofon {
		go activateProfiling(opt.pprofdir, time.Duration(opt.pprofduration)*time.Second)
	}

	flow.Acknowledger(consumer, ack_chan)
}

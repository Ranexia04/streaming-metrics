package main

import (
	"github.com/jnovack/flag"
)

type opt struct {
	pulsarUrl                     string
	pulsarTopic                   string
	pulsarSubscription            string
	pulsarConsumer                string
	pulsarTrustCertsFile          string
	pulsarCertFile                string
	pulsarKeyFile                 string
	pulsarAllowInsecureConnection bool

	consumerThreads uint

	metricsDir string

	pprofon       bool
	pprofdir      string
	pprofduration uint

	prometheusPort                uint
	activateObserveProcessingTime bool

	logLevel string

	tickerSeconds uint
}

func loadArgs() opt {
	var opt opt

	flag.StringVar(&opt.pulsarUrl, "pulsar_url", "pulsar://localhost:6650", "Source pulsar address")
	flag.StringVar(&opt.pulsarTopic, "pulsar_topic", "persistent://public/default/in", "Source topic names (seperated by ;)")
	flag.StringVar(&opt.pulsarConsumer, "pulsar_consumer", "streaming_metrics_consumer", "Source consumer name")
	flag.StringVar(&opt.pulsarSubscription, "pulsar_subscription", "streaming_metrics", "Source subscription name")
	flag.StringVar(&opt.pulsarTrustCertsFile, "pulsar_trust_certs_file", "", "Path for source pem file, for ca.cert")
	flag.StringVar(&opt.pulsarCertFile, "pulsar_cert_file", "", "Path for source cert.pem file")
	flag.StringVar(&opt.pulsarKeyFile, "pulsar_key_file", "", "Path for source key-pk8.pem file")
	flag.BoolVar(&opt.pulsarAllowInsecureConnection, "pulsar_allow_insecure_connection", false, "Source allow insecure connection")

	flag.UintVar(&opt.consumerThreads, "consumer_threads", 6, "Number of threads to consume from pulsar")

	flag.StringVar(&opt.metricsDir, "metrics_dir", "./metrics", "Directory of all the jq metrics files")

	flag.BoolVar(&opt.pprofon, "pprof_on", false, "Profoling on?")
	flag.StringVar(&opt.pprofdir, "pprof_dir", "./pprof", "Directory for pprof file")
	flag.UintVar(&opt.pprofduration, "pprof_duration", 60*2, "Number of seconds to run pprof")

	flag.UintVar(&opt.prometheusPort, "prometheus_port", 7700, "Prometheous port")
	flag.BoolVar(&opt.activateObserveProcessingTime, "activate_timing_collection", false, "Is the collection by prometheus of processing time on (may hinder perforance!)")

	flag.StringVar(&opt.logLevel, "log_level", "info", "Logging level: panic - fatal - error - warn - info - debug - trace")

	flag.UintVar(&opt.tickerSeconds, "ticker_seconds", 1, "tickerseconds")

	flag.Parse()

	return opt
}

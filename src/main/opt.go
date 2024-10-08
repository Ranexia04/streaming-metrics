package main

import (
	"github.com/jnovack/flag"
)

type opt struct {
	sourcePulsar                  string
	sourceTopic                   string
	sourceSubscription            string
	sourceName                    string
	sourceTrustCerts              string
	sourceCertFile                string
	sourceKeyFile                 string
	sourceAllowInsecureConnection bool

	consumerThreads uint

	metricsDir string

	pprofon       bool
	pprofdir      string
	pprofduration uint

	prometheusPort                uint
	activateObserveProcessingTime bool

	loglevel string

	tickerseconds uint
}

func loadArgs() opt {
	var opt opt

	flag.StringVar(&opt.sourcePulsar, "source_pulsar", "pulsar://localhost:6650", "Source pulsar address")
	flag.StringVar(&opt.sourceTopic, "source_topic", "persistent://public/default/in", "Source topic names (seperated by ;)")
	flag.StringVar(&opt.sourceSubscription, "source_subscription", "streaming_metrics", "Source subscription name")
	flag.StringVar(&opt.sourceName, "source_name", "streaming_metrics_consumer", "Source consumer name")
	flag.StringVar(&opt.sourceTrustCerts, "source_trust_certs", "", "Path for source pem file, for ca.cert")
	flag.StringVar(&opt.sourceCertFile, "source_cert_file", "", "Path for source cert.pem file")
	flag.StringVar(&opt.sourceKeyFile, "source_key_file", "", "Path for source key-pk8.pem file")
	flag.BoolVar(&opt.sourceAllowInsecureConnection, "source_allow_insecure_connection", false, "Source allow insecure connection")

	flag.UintVar(&opt.consumerThreads, "consumer_threads", 6, "Number of threads to consume from pulsar")

	flag.StringVar(&opt.metricsDir, "metrics_dir", "./metrics", "Directory of all the jq metrics files")

	flag.BoolVar(&opt.pprofon, "pprof_on", false, "Profoling on?")
	flag.StringVar(&opt.pprofdir, "pprof_dir", "./pprof", "Directory for pprof file")
	flag.UintVar(&opt.pprofduration, "pprof_duration", 60*2, "Number of seconds to run pprof")

	flag.UintVar(&opt.prometheusPort, "prometheus_port", 7700, "Prometheous port")
	flag.BoolVar(&opt.activateObserveProcessingTime, "activatete_timing_colection", false, "Is the collection by prometheus of processing time on (may hinder perforance!)")

	flag.StringVar(&opt.loglevel, "log_level", "info", "Logging level: panic - fatal - error - warn - info - debug - trace")

	flag.UintVar(&opt.tickerseconds, "ticker_seconds", 1, "tickerseconds")

	flag.Parse()

	return opt
}

package main

import "os"

type tracingConfig struct {
	Enable                  bool
	JaegerAgentEndpoint     string
	JaegerCollectorEndpoint string
}

func newTracingConfig() tracingConfig {
	var enable bool
	if os.Getenv("ENABLE_TRACING") != "" {
		enable = true
	}
	enable = true
	tconf := tracingConfig{
		Enable:                  enable,
		JaegerAgentEndpoint:     os.Getenv("JAEGER_AGENT_ENDPOINT"),
		JaegerCollectorEndpoint: os.Getenv("JAEGER_COLLECTOR_ENDPOINT"),
	}
	return tconf
}

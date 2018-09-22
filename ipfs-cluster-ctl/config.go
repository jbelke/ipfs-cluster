package main

import "os"

type tracingConfig struct {
	Enable                  bool
	JaegerAgentEndpoint     string
	JaegerCollectorEndpoint string
}

func newTracingConfig() tracingConfig {
	enablestr := os.Getenv("ENABLE_TRACING")
	var enable bool
	if enablestr == "" {
		enable = false
	}
	enable = true
	return tracingConfig{
		Enable:                  enable,
		JaegerAgentEndpoint:     os.Getenv("JAEGER_AGENT_ENDPOINT"),
		JaegerCollectorEndpoint: os.Getenv("JAEGER_COLLECTOR_ENDPOINT"),
	}
}

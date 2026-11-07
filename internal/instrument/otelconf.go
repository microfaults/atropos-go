package instrument

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/otelconf"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
)

const (
	// configFileEnv names the OpenTelemetry declarative configuration file.
	configFileEnv = "OTEL_CONFIG_FILE"
	// deprecatedConfigFileEnv is the variable's former name, which otelconf
	// v0.26.0 no longer accepts.
	deprecatedConfigFileEnv = "OTEL_EXPERIMENTAL_CONFIG_FILE"
)

// declarativeConfig builds the SDK described by the declarative configuration
// file OTEL_CONFIG_FILE names (format 1.0-rc.2). sdk is nil, with a nil error,
// when OTEL_CONFIG_FILE is unset or blank; its value is read with surrounding
// whitespace trimmed, like the other OTEL_* variables the init reads, and a
// padded value is written back trimmed, because otelconf.NewSDK reads the
// variable itself, untrimmed. meterSection reports whether the file has a
// meter_provider section (see declarativeTelemetry). The error is non-nil when
// OTEL_EXPERIMENTAL_CONFIG_FILE holds a value (it is rejected in favour of
// OTEL_CONFIG_FILE, whatever OTEL_CONFIG_FILE says), when the file cannot be
// read or parsed, or when otelconf cannot build the SDK from it.
//
// otelconf.NewSDK looks both variables up itself and rejects a set but empty
// OTEL_EXPERIMENTAL_CONFIG_FILE, so an empty one is removed from the process
// environment before the SDK is built; it re-reads OTEL_CONFIG_FILE too, and
// its own read of the file supersedes the configuration passed here. When it
// fails on a later provider, NewSDK does not shut down the providers it had
// already built (otelconf v0.26.0): their exporters are left to the process,
// which fails to start anyway.
func declarativeConfig(ctx context.Context) (sdk *otelconf.SDK, meterSection bool, err error) {
	if os.Getenv(deprecatedConfigFileEnv) != "" {
		return nil, false, fmt.Errorf("atropos: %s is no longer supported, use %s instead", deprecatedConfigFileEnv, configFileEnv)
	}
	raw := os.Getenv(configFileEnv)
	path := strings.TrimSpace(raw)
	if path == "" {
		return nil, false, nil
	}
	if path != raw {
		if err := os.Setenv(configFileEnv, path); err != nil {
			return nil, false, fmt.Errorf("atropos: trim %s: %w", configFileEnv, err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("atropos: read %s: %w", configFileEnv, err)
	}
	cfg, err := otelconf.ParseYAML(b)
	if err != nil {
		return nil, false, fmt.Errorf("atropos: parse %s %q: %w", configFileEnv, path, err)
	}
	if _, set := os.LookupEnv(deprecatedConfigFileEnv); set {
		if err := os.Unsetenv(deprecatedConfigFileEnv); err != nil {
			return nil, false, fmt.Errorf("atropos: unset empty %s: %w", deprecatedConfigFileEnv, err)
		}
	}
	built, err := otelconf.NewSDK(otelconf.WithContext(ctx), otelconf.WithOpenTelemetryConfiguration(*cfg))
	if err != nil {
		return nil, false, fmt.Errorf("atropos: build SDK from %s %q: %w", configFileEnv, path, err)
	}
	return &built, cfg.MeterProvider != nil, nil
}

// declarativeTelemetry registers the file-built SDK's tracer and logger
// providers globally and assembles the Telemetry on them: logger is the host's
// logger, nil building NewLogger(os.Stdout, the file's logger provider), and
// Shutdown is the SDK's, run once. Two sections of the file are not used, and
// each one present is named once by a WARN through the returned Logger: the
// propagators stay the ones initTelemetry set, and metrics stay on
// client_golang, so the meter provider a meter_provider section builds is never
// registered and its readers export nothing the SDK records (they are still
// started, and stopped by Shutdown).
func declarativeTelemetry(sdk *otelconf.SDK, meterSection bool, logger *slog.Logger) *Telemetry {
	otel.SetTracerProvider(sdk.TracerProvider())
	global.SetLoggerProvider(sdk.LoggerProvider())
	tel := newTelemetry(logger, sdk.LoggerProvider(), shutdownAll(sdk.Shutdown))
	file := os.Getenv(configFileEnv)
	if len(sdk.Propagator().Fields()) > 0 {
		tel.Logger.Warn("atropos: the propagator section of "+configFileEnv+" is ignored; W3C TraceContext and Baggage stay in use",
			"file", file)
	}
	if meterSection {
		tel.Logger.Warn("atropos: the meter_provider section of "+configFileEnv+" is ignored; metrics stay on client_golang (/metrics), so its readers export nothing the SDK records",
			"file", file)
	}
	return tel
}

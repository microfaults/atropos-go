package instrument

import (
	"context"
	"fmt"
	"log/slog"
	"os"

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
// file OTEL_CONFIG_FILE names (format 1.0-rc.2). ok is false, with a nil error,
// when OTEL_CONFIG_FILE is unset or empty. The error is non-nil when
// OTEL_EXPERIMENTAL_CONFIG_FILE holds a value (it is rejected in favour of
// OTEL_CONFIG_FILE, whatever OTEL_CONFIG_FILE says), when the file cannot be
// read or parsed, or when otelconf cannot build the SDK from it.
//
// otelconf.NewSDK looks both variables up itself and rejects a set but empty
// OTEL_EXPERIMENTAL_CONFIG_FILE, so an empty one is removed from the process
// environment before the SDK is built; it re-reads OTEL_CONFIG_FILE too, and
// its own read of the file supersedes the configuration passed here.
func declarativeConfig(ctx context.Context) (sdk *otelconf.SDK, ok bool, err error) {
	if os.Getenv(deprecatedConfigFileEnv) != "" {
		return nil, false, fmt.Errorf("atropos: %s is no longer supported, use %s instead", deprecatedConfigFileEnv, configFileEnv)
	}
	path := os.Getenv(configFileEnv)
	if path == "" {
		return nil, false, nil
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
	return &built, true, nil
}

// declarativeTelemetry registers the file-built SDK's tracer and logger
// providers globally and assembles the Telemetry on them: logger is the host's
// logger, nil building NewLogger(os.Stdout, the file's logger provider), and
// Shutdown is the SDK's, run once. The propagators stay the ones initTelemetry
// set: when the file configures any, that section is ignored and a WARN says so
// once, through the returned Logger.
func declarativeTelemetry(sdk *otelconf.SDK, logger *slog.Logger) *Telemetry {
	otel.SetTracerProvider(sdk.TracerProvider())
	global.SetLoggerProvider(sdk.LoggerProvider())
	tel := newTelemetry(logger, sdk.LoggerProvider(), shutdownAll(sdk.Shutdown))
	if len(sdk.Propagator().Fields()) > 0 {
		tel.Logger.Warn("atropos: the propagator section of "+configFileEnv+" is ignored; W3C TraceContext and Baggage stay in use",
			"file", os.Getenv(configFileEnv))
	}
	return tel
}

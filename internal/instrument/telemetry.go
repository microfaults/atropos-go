package instrument

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	// The one semconv version for both hosts; the sidecar shares it through InitTelemetry.
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// telemetryConfig is the OTel bootstrap input. Serve fills it from Config;
// tests fill it directly (tracerProvider is the hermetic seam).
type telemetryConfig struct {
	serviceName    string
	serviceVersion string                   // "" leaves service.version to OTEL_RESOURCE_ATTRIBUTES
	environment    string                   // "" leaves deployment.environment.name to OTEL_RESOURCE_ATTRIBUTES
	instanceID     string                   // the registration identity; "" falls back to instanceIdentity
	host           string                   // HostInProcess or HostSidecar; "" omits atropos.host
	sampler        sdktrace.Sampler         // tests only; nil leaves it to OTEL_TRACES_SAMPLER(_ARG)
	tracerProvider oteltrace.TracerProvider // BYO: registered globally, caller owns shutdown, no exporters built
	loggerProvider otellog.LoggerProvider   // BYO logs, honoured only with tracerProvider: registered globally
	logger         *slog.Logger             // BYO logger; nil = NewLogger(os.Stdout, the logger provider)
}

// Telemetry is what InitTelemetry hands a host.
type Telemetry struct {
	// Logger is the host's logger: TelemetryConfig.Logger, else
	// NewLogger(os.Stdout, LoggerProvider).
	Logger *slog.Logger
	// LoggerProvider is the SDK's provider, the BYO one, or, when the host
	// brought only a TracerProvider, the global provider as found.
	LoggerProvider otellog.LoggerProvider
	// Shutdown flushes and stops the SDK's tracer and logger providers and
	// joins their errors; later calls return the first call's result. It is a
	// no-op when the host brought its own TracerProvider.
	Shutdown func(context.Context) error
}

// initTelemetry bootstraps OpenTelemetry for the calling service: the
// propagators, the tracer and logger providers (registered globally), and the
// default logger.
func initTelemetry(ctx context.Context, cfg telemetryConfig) (*Telemetry, error) {
	// Always set propagators regardless of BYO path.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// BYO TracerProvider path: register the host's providers and build no
	// exporter of any kind, for traces or logs. The host owns their shutdown.
	if cfg.tracerProvider != nil {
		otel.SetTracerProvider(cfg.tracerProvider)
		lp := cfg.loggerProvider
		if lp != nil {
			global.SetLoggerProvider(lp)
		} else {
			lp = global.GetLoggerProvider()
		}
		return newTelemetry(cfg.logger, lp, func(context.Context) error { return nil }), nil
	}

	// A declarative configuration file wins over every OTEL_* exporter
	// variable and owns the resource (see declarativeConfig).
	sdk, meterSection, err := declarativeConfig(ctx)
	if err != nil {
		return nil, err
	}
	if sdk != nil {
		return declarativeTelemetry(sdk, meterSection, cfg.logger), nil
	}

	res, err := newResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("atropos: build resource: %w", err)
	}

	// The exporters are chosen by the standard OTEL_* environment (see
	// newSpanExporter and newLogExporter).
	spanExporter, err := newSpanExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("atropos: init span exporter: %w", err)
	}
	logExporter, err := newLogExporter(ctx)
	if err != nil {
		_ = spanExporter.Shutdown(ctx)
		return nil, fmt.Errorf("atropos: init log exporter: %w", err)
	}

	// Build TracerProvider. With no explicit sampler the SDK reads
	// OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG itself and defaults to
	// ParentBased(AlwaysSample).
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(spanExporter, sdktrace.WithBatchTimeout(100*time.Millisecond)),
		sdktrace.WithResource(res),
	}
	if cfg.sampler != nil { // tests only; production reads OTEL_TRACES_SAMPLER(_ARG)
		opts = append(opts, sdktrace.WithSampler(cfg.sampler))
	}
	tp := sdktrace.NewTracerProvider(opts...)

	// Logs share the resource; the batch processor reads OTEL_BLRP_*.
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	global.SetLoggerProvider(lp)

	return newTelemetry(cfg.logger, lp, shutdownAll(tp.Shutdown, lp.Shutdown)), nil
}

// newTelemetry assembles the result, building the default logger on lp when
// the host brought none.
func newTelemetry(logger *slog.Logger, lp otellog.LoggerProvider, shutdown func(context.Context) error) *Telemetry {
	if logger == nil {
		logger = NewLogger(os.Stdout, lp)
	}
	return &Telemetry{Logger: logger, LoggerProvider: lp, Shutdown: shutdown}
}

// shutdownAll returns a shutdown that runs every step once, in order, also
// after a step fails, and joins their errors. Later calls run nothing and
// return the first call's result.
func shutdownAll(steps ...func(context.Context) error) func(context.Context) error {
	var once sync.Once
	var err error
	return func(ctx context.Context) error {
		once.Do(func() {
			errs := make([]error, 0, len(steps))
			for _, step := range steps {
				errs = append(errs, step(ctx))
			}
			err = errors.Join(errs...)
		})
		return err
	}
}

// newResource describes this process on every span and log record: the
// service identity from cfg (service.name, "unknown" when empty; service.version
// and deployment.environment.name only when set, so OTEL_RESOURCE_ATTRIBUTES
// can supply them), service.instance.id (cfg.instanceID, the identity the host
// registers under, else instanceIdentity's), k8s.pod.name when there is a pod
// name and, when the host is set, atropos.host, merged over resource.Default(),
// which already folds in OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME. The
// attributes set here win over the environment's.
func newResource(cfg telemetryConfig) (*resource.Resource, error) {
	serviceName := cfg.serviceName
	if serviceName == "" {
		serviceName = "unknown"
	}
	instanceID, podName := instanceIdentity(os.Getenv)
	if cfg.instanceID != "" {
		instanceID = cfg.instanceID // verbatim: spans join manteion's instance rows on it
	}

	attrs := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
		semconv.ServiceInstanceID(instanceID),
	}
	if cfg.serviceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.serviceVersion))
	}
	if cfg.environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentName(cfg.environment))
	}
	if podName != "" {
		attrs = append(attrs, semconv.K8SPodName(podName))
	}
	if cfg.host != "" {
		attrs = append(attrs, attribute.String(trace.AttrHost, cfg.host))
	}

	// Use NewSchemaless to avoid schema URL conflicts between
	// resource.Default() (which tracks the SDK's semconv version)
	// and the semconv import version used here.
	return resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))
}

// instanceIdentity names this process for the resource. The pod name is
// ATROPOS_POD_NAME, else HOSTNAME (which Kubernetes sets to the pod name), and
// is empty when neither is set. The instance id, used when the host passes
// none, is the pod name when there is one, else the machine's hostname, else
// "unknown".
func instanceIdentity(getenv func(string) string) (instanceID, podName string) {
	podName = strings.TrimSpace(getenv("ATROPOS_POD_NAME"))
	if podName == "" {
		podName = strings.TrimSpace(getenv("HOSTNAME"))
	}
	if podName != "" {
		return podName, podName
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host, ""
	}
	return "unknown", ""
}

// applyExporterEnvFallback maps the bed's COLLECTOR_SERVICE_ADDR onto the
// standard OTLP variables when they are unset, and keeps the legacy
// schemeless endpoint form working: the OTLP exporters parse
// OTEL_EXPORTER_OTLP_ENDPOINT as a URL (a bare host:port yields an empty
// host), and autoexport defaults to http/protobuf while every schemeless
// endpoint this SDK ever accepted was a gRPC collector port. It mutates the
// process environment so autoexport and the exporters see one truth.
//
// An OTEL_EXPORTER_OTLP_ENDPOINT that is already a URL is left alone, and so
// is an environment that names no endpoint at all (the exporters' own
// defaults apply). Otherwise the endpoint gets an http:// scheme if it lacks
// one and the protocol becomes grpc unless the environment already names one.
// The result holds the variables set and their values, empty when nothing
// changed; a second call changes nothing. Values are read with surrounding
// whitespace trimmed, as the OTLP exporters read theirs, and a blank value
// counts as unset. A padded OTEL_EXPORTER_OTLP_PROTOCOL is written back
// trimmed whatever the endpoint, because autoexport compares it untrimmed.
func applyExporterEnvFallback() map[string]string {
	const (
		endpointKey = "OTEL_EXPORTER_OTLP_ENDPOINT"
		protocolKey = "OTEL_EXPORTER_OTLP_PROTOCOL"
		addrKey     = "COLLECTOR_SERVICE_ADDR"
	)
	applied := map[string]string{}
	set := func(key, value string) {
		// Ignoring the error is safe: the keys are constants, so only a NUL byte can fail.
		if err := os.Setenv(key, value); err == nil {
			applied[key] = value
		}
	}

	if protocol := os.Getenv(protocolKey); protocol != strings.TrimSpace(protocol) {
		set(protocolKey, strings.TrimSpace(protocol))
	}

	explicit := strings.TrimSpace(os.Getenv(endpointKey))
	endpoint := explicit
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv(addrKey))
	}
	if endpoint == "" || strings.Contains(explicit, "://") {
		// No endpoint named, or the standard variable already holds a URL.
		return applied
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	set(endpointKey, endpoint)
	if strings.TrimSpace(os.Getenv(protocolKey)) == "" {
		set(protocolKey, "grpc")
	}
	return applied
}

// newSpanExporter builds the span exporter the environment selects:
// OTEL_TRACES_EXPORTER (otlp by default, console, none) and, for otlp,
// OTEL_EXPORTER_OTLP_PROTOCOL and the other OTEL_EXPORTER_OTLP_* variables,
// after applyExporterEnvFallback.
func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	applyExporterEnvFallback()
	return autoexport.NewSpanExporter(ctx)
}

// newLogExporter builds the log exporter the environment selects:
// OTEL_LOGS_EXPORTER (otlp by default, console, none) and, for otlp,
// OTEL_EXPORTER_OTLP_PROTOCOL and the other OTEL_EXPORTER_OTLP_* variables,
// after the same applyExporterEnvFallback as the span exporter.
func newLogExporter(ctx context.Context) (sdklog.Exporter, error) {
	applyExporterEnvFallback()
	return autoexport.NewLogExporter(ctx)
}

// TelemetryConfig is the host-facing OTel bootstrap input. Traces and logs are
// both signals. Their exporters come from the environment through contrib
// autoexport: OTEL_TRACES_EXPORTER and OTEL_LOGS_EXPORTER (otlp by default,
// console, none), OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL (grpc
// or http/protobuf; autoexport defaults to http/protobuf) and the other
// OTEL_EXPORTER_OTLP_* variables the exporters read. The bed's
// COLLECTOR_SERVICE_ADDR, and a schemeless OTEL_EXPORTER_OTLP_ENDPOINT such as
// otel-collector:4317, still work and mean a gRPC collector. The sampler comes
// from OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG; the default is
// parent-based always-on. The resource on every span and log record carries
// service.name and service.instance.id, service.version and
// deployment.environment.name when set, k8s.pod.name (ATROPOS_POD_NAME, else
// HOSTNAME; omitted when neither is set) and atropos.host (Host; omitted when
// empty). OTEL_RESOURCE_ATTRIBUTES adds further attributes, and supplies the
// version and environment left empty here. Host, the sampler and the resource
// apply only when no TracerProvider is given: a BYO provider keeps its own.
// Without one, OTEL_CONFIG_FILE replaces all of this with the declarative
// configuration file it names (see declarativeConfig): the file owns the
// exporters, the sampler and the whole resource, and the propagators stay.
type TelemetryConfig struct {
	ServiceName string
	// ServiceVersion is service.version; empty leaves it to
	// OTEL_RESOURCE_ATTRIBUTES.
	ServiceVersion string
	// Environment is deployment.environment.name; empty leaves it to
	// OTEL_RESOURCE_ATTRIBUTES.
	Environment string
	// InstanceID is service.instance.id, and should be the identity the host
	// registers under (ResolveInstanceID) so spans and logs join manteion's
	// instance. Empty falls back to ATROPOS_POD_NAME, else HOSTNAME, else the
	// machine's hostname.
	InstanceID string
	// Host says which kind of atropos host this is, HostInProcess or
	// HostSidecar. It becomes the atropos.host resource attribute; empty omits it.
	Host string
	// TracerProvider, if set, is registered globally and no exporter is built,
	// for traces or logs: the seam for tests and for hosts that own their OTel
	// setup, and with it its shutdown.
	TracerProvider oteltrace.TracerProvider
	// LoggerProvider, if set, is registered as the global logger provider and
	// feeds the default logger's OTel half. It is ignored unless
	// TracerProvider is also set: a host that owns one signal owns both.
	LoggerProvider otellog.LoggerProvider
	// Logger, if set, is returned as Telemetry.Logger; nil builds the SDK
	// default with NewLogger on stdout.
	Logger *slog.Logger
}

// InitTelemetry bootstraps OpenTelemetry for a host: traces and logs exported
// as the environment selects (or the host's own providers), the default
// logger, and the shutdown that flushes both signals.
func InitTelemetry(ctx context.Context, cfg TelemetryConfig) (*Telemetry, error) {
	return initTelemetry(ctx, telemetryConfig{
		serviceName:    cfg.ServiceName,
		serviceVersion: cfg.ServiceVersion,
		environment:    cfg.Environment,
		instanceID:     cfg.InstanceID,
		host:           cfg.Host,
		tracerProvider: cfg.TracerProvider,
		loggerProvider: cfg.LoggerProvider,
		logger:         cfg.Logger,
	})
}

package instrument

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const minimalConfig = `file_format: "1.0-rc.2"
resource:
  attributes:
    - name: service.name
      value: from-file
tracer_provider:
  processors: []
logger_provider:
  processors: []
`

func TestInitTelemetry_DeclarativeConfigWins(t *testing.T) {
	restoreOTelGlobals(t)
	path := filepath.Join(t.TempDir(), "otel.yaml")
	if err := os.WriteFile(path, []byte(minimalConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_CONFIG_FILE", path)
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "")
	t.Setenv("OTEL_TRACES_EXPORTER", "console") // must be ignored: the file wins
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "from-config"})
	if err != nil {
		t.Fatal(err)
	}
	defer tel.Shutdown(context.Background())
	if v, ok := resourceOf(t).Set().Value("service.name"); !ok || v.AsString() != "from-file" {
		t.Fatalf("service.name = %v, want from-file (the file owns the resource)", v)
	}
	if _, ok := global.GetLoggerProvider().(*sdklog.LoggerProvider); !ok {
		t.Fatalf("logger provider = %T, want the file's SDK provider", global.GetLoggerProvider())
	}
}

func TestDeclarativeConfig_RejectsDeprecatedVariable(t *testing.T) {
	t.Setenv("OTEL_CONFIG_FILE", "")
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "/nonexistent.yaml")
	if _, _, err := declarativeConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "OTEL_CONFIG_FILE") {
		t.Fatalf("err = %v, want a pointer to OTEL_CONFIG_FILE", err)
	}
}

func TestDeclarativeConfig_UnsetIsNotOK(t *testing.T) {
	t.Setenv("OTEL_CONFIG_FILE", "")
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "")
	if sdk, _, err := declarativeConfig(context.Background()); sdk != nil || err != nil {
		t.Fatalf("sdk=%v err=%v", sdk, err)
	}
}

// writeConfig writes a declarative configuration file and points
// OTEL_CONFIG_FILE at it.
func writeConfig(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "otel.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_CONFIG_FILE", path)
	t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "")
	return path
}

// The sections of the file the SDK does not use are each named by one WARN
// through the Telemetry's logger, and only when present.
func TestInitTelemetry_DeclarativeIgnoredSectionsWarnOnce(t *testing.T) {
	const propagatorSection = "propagator:\n  composite:\n    - tracecontext:\n"
	const meterSection = "meter_provider:\n  readers: []\n"
	for name, tc := range map[string]struct {
		extra             string
		propagator, meter bool
	}{
		"neither":    {"", false, false},
		"propagator": {propagatorSection, true, false},
		"meter":      {meterSection, false, true},
		"both":       {propagatorSection + meterSection, true, true},
	} {
		t.Run(name, func(t *testing.T) {
			restoreOTelGlobals(t)
			writeConfig(t, minimalConfig+tc.extra)
			var buf bytes.Buffer
			tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "warn-test", logger: slog.New(slog.NewTextHandler(&buf, nil))})
			if err != nil {
				t.Fatal(err)
			}
			defer tel.Shutdown(context.Background())
			out := buf.String()
			for section, want := range map[string]bool{"the propagator section": tc.propagator, "the meter_provider section": tc.meter} {
				if got := strings.Count(out, section); got != map[bool]int{true: 1, false: 0}[want] {
					t.Errorf("%q warned %d times, want present=%v; log:\n%s", section, got, want, out)
				}
			}
			if tc.meter && !strings.Contains(out, "client_golang") {
				t.Errorf("the meter_provider WARN does not say metrics stay on client_golang:\n%s", out)
			}
		})
	}
}

// An unreadable file and one that does not parse fail the init, each naming
// OTEL_CONFIG_FILE.
func TestDeclarativeConfig_UnreadableAndUnparsableFail(t *testing.T) {
	t.Run("unreadable", func(t *testing.T) {
		t.Setenv("OTEL_CONFIG_FILE", filepath.Join(t.TempDir(), "missing.yaml"))
		t.Setenv("OTEL_EXPERIMENTAL_CONFIG_FILE", "")
		if sdk, _, err := declarativeConfig(context.Background()); sdk != nil || err == nil || !strings.Contains(err.Error(), "read OTEL_CONFIG_FILE") {
			t.Fatalf("sdk=%v err=%v, want a read error naming OTEL_CONFIG_FILE", sdk, err)
		}
	})
	t.Run("unparsable", func(t *testing.T) {
		writeConfig(t, "file_format: [not, a, string\n")
		if sdk, _, err := declarativeConfig(context.Background()); sdk != nil || err == nil || !strings.Contains(err.Error(), "parse OTEL_CONFIG_FILE") {
			t.Fatalf("sdk=%v err=%v, want a parse error naming OTEL_CONFIG_FILE", sdk, err)
		}
	})
}

// A padded OTEL_CONFIG_FILE is trimmed, for this read and for otelconf's own.
func TestDeclarativeConfig_TrimsThePath(t *testing.T) {
	restoreOTelGlobals(t)
	path := writeConfig(t, minimalConfig)
	t.Setenv("OTEL_CONFIG_FILE", "  "+path+"\n")
	sdk, _, err := declarativeConfig(context.Background())
	if err != nil || sdk == nil {
		t.Fatalf("sdk=%v err=%v, want the padded path to load", sdk, err)
	}
	defer sdk.Shutdown(context.Background())
	if got := os.Getenv("OTEL_CONFIG_FILE"); got != path {
		t.Fatalf("OTEL_CONFIG_FILE = %q, want it written back trimmed (%q)", got, path)
	}
}

// Telemetry.Shutdown on the declarative path shuts the file-built SDK down:
// the tracer provider it registered records spans before and none after.
func TestInitTelemetry_DeclarativeShutdownStopsTheSDK(t *testing.T) {
	restoreOTelGlobals(t)
	writeConfig(t, minimalConfig)
	tel, err := initTelemetry(context.Background(), telemetryConfig{serviceName: "shutdown-test", logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	tp := otel.GetTracerProvider()
	if _, ok := tp.(*sdktrace.TracerProvider); !ok {
		t.Fatalf("tracer provider = %T, want the file's SDK provider", tp)
	}
	_, before := tp.Tracer("t").Start(context.Background(), "before")
	before.End()
	if !before.SpanContext().IsValid() {
		t.Fatal("the file's tracer provider does not record before Shutdown")
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	_, after := tp.Tracer("t").Start(context.Background(), "after")
	defer after.End()
	if after.SpanContext().IsValid() {
		t.Fatal("the file's tracer provider still mints spans after Telemetry.Shutdown: the SDK was not shut down")
	}
}

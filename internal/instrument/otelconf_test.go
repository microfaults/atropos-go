package instrument

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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
	if _, ok, err := declarativeConfig(context.Background()); ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

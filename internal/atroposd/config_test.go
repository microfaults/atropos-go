package atroposd

import (
	"git.ucsc.edu/microfaults/atropos-go/internal/agent"

	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "atroposd.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"ATROPOS_SERVICE", "MANTEION_SERVICE_VERSION", "MANTEION_URL", "MANTEION_ADVERTISE_ADDR",
		"MANTEION_INSTANCE_ID", "ATROPOSD_CONTROL_ADDR", "ATROPOSD_INGRESS_ADDR", "ATROPOSD_INBOUND_UPSTREAM",
		"ATROPOSD_CAPTURE", "ATROPOSD_EGRESS", "ATROPOSD_EGRESS_ADDR", "ATROPOSD_APP_HOST", "ATROPOSD_CAPTURE_MARKER",
		"ATROPOSD_INBOUND_PORTS", "ATROPOSD_RAW_TCP_PORTS", "ATROPOSD_AGENT_SOCKET", "ATROPOSD_APP_CONTAINER", "ATROPOS_POD_UID"} {
		t.Setenv(k, "")
	}
}

func TestLoad_YAMLThenEnvThenDefaults(t *testing.T) {
	clearEnv(t)
	p := writeYAML(t, `
service: productcatalogservice
manteion_url: http://manteion:8080
egress:
  - listen: ":19090"
    upstream: currencyservice:7000
routes:
  - method: GET
    path: /products/{id}
    depends_on: ["GET /products"]
`)
	t.Setenv("MANTEION_URL", "http://override:9090")
	t.Setenv("ATROPOSD_EGRESS", ":19090=productcatalogservice:3550, :19091=currencyservice:7000")

	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Service != "productcatalogservice" {
		t.Fatalf("service = %q (from yaml)", cfg.Service)
	}
	if cfg.ManteionURL != "http://override:9090" {
		t.Fatalf("manteion_url = %q, want the environment to win", cfg.ManteionURL)
	}
	if len(cfg.Egress) != 2 || cfg.Egress[0] != (EgressRoute{":19090", "productcatalogservice:3550"}) || cfg.Egress[1] != (EgressRoute{":19091", "currencyservice:7000"}) {
		t.Fatalf("egress = %+v, want the ATROPOSD_EGRESS pairs", cfg.Egress)
	}
	if cfg.ControlListen != DefaultControlListen || cfg.InboundUpstream != DefaultInboundUpstream || cfg.Capture != CaptureExplicit {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	if rs := cfg.wireRoutes(); len(rs) != 1 || rs[0].Path != "/products/{id}" || len(rs[0].DependsOn) != 1 {
		t.Fatalf("routes = %+v", rs)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestValidate_ReportsEveryProblem(t *testing.T) {
	clearEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("empty config must not validate")
	}
	for _, want := range []string{"service is required", "at least one egress listener"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}

	cfg.Service = "svc"
	cfg.Capture = CaptureTransparent // set after Load: no egress_listen default applied
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "egress_listen") {
		t.Fatalf("transparent capture without egress_listen must be rejected, got %v", err)
	}
	cfg.Capture = "magic"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "unknown capture mode") {
		t.Fatalf("unknown capture must be rejected, got %v", err)
	}
	cfg.Capture = CaptureExplicit
	cfg.Egress = []EgressRoute{{Listen: ":1"}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "egress[0]") {
		t.Fatalf("half-specified egress must be rejected, got %v", err)
	}
}

func TestLoad_TransparentDefaultsAndPortLists(t *testing.T) {
	clearEnv(t)
	t.Setenv("ATROPOS_SERVICE", "cartservice")
	t.Setenv("ATROPOSD_CAPTURE", "transparent")
	t.Setenv("ATROPOSD_INBOUND_PORTS", "7070")
	t.Setenv("ATROPOSD_RAW_TCP_PORTS", "6379, 9092")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EgressListen != DefaultEgressListen || cfg.IngressListen != DefaultIngressListen || cfg.AppHost != DefaultAppHost || cfg.CaptureMarker != DefaultCaptureMarker {
		t.Fatalf("transparent defaults not applied: %+v", cfg)
	}
	if len(cfg.InboundPorts) != 1 || cfg.InboundPorts[0] != 7070 || len(cfg.RawTCPPorts) != 2 || cfg.RawTCPPorts[1] != 9092 {
		t.Fatalf("ports = %v / %v", cfg.InboundPorts, cfg.RawTCPPorts)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	t.Setenv("ATROPOSD_RAW_TCP_PORTS", "redis")
	cfg, _ = Load("")
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "not a valid TCP port") {
		t.Fatalf("junk port list must be rejected, got %v", err)
	}
}

func TestLoad_AgentFieldsDefaultThenEnv(t *testing.T) {
	clearEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentSocket != agent.DefaultSocket || cfg.AppContainer != DefaultAppContainer || cfg.PodUID != "" {
		t.Fatalf("agent defaults: socket=%q container=%q pod_uid=%q", cfg.AgentSocket, cfg.AppContainer, cfg.PodUID)
	}
	t.Setenv("ATROPOSD_AGENT_SOCKET", "/run/x/agent.sock")
	t.Setenv("ATROPOSD_APP_CONTAINER", "app")
	t.Setenv("ATROPOS_POD_UID", "6d3f9a4e-0000-4000-8000-000000000001")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentSocket != "/run/x/agent.sock" || cfg.AppContainer != "app" || cfg.PodUID != "6d3f9a4e-0000-4000-8000-000000000001" {
		t.Fatalf("agent env overlay not applied: socket=%q container=%q pod_uid=%q", cfg.AgentSocket, cfg.AppContainer, cfg.PodUID)
	}
}

package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
)

// testInstrument is an offline core with its control surface served by
// httptest, so tests drive rules through the same HTTP surface manteion uses.
type testInstrument struct {
	core    *instrument.Core
	control *httptest.Server
}

func newTestInstrument(t *testing.T) *testInstrument {
	t.Helper()
	core := instrument.NewCore(instrument.CoreConfig{Service: "svc", Instance: "pod-1", Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(core.Stop)
	control := httptest.NewServer(instrument.ControlSurface(instrument.ControlConfig{Core: core}))
	t.Cleanup(control.Close)
	return &testInstrument{core: core, control: control}
}

func (ti *testInstrument) resolve() *interceptor.Interceptor { return ti.core.Interceptor() }

func (ti *testInstrument) post(t *testing.T, path, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(ti.control.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (ti *testInstrument) rules(t *testing.T, rulesJSON string) {
	t.Helper()
	if code, body := ti.post(t, "/admin/rules", rulesJSON); code != http.StatusNoContent {
		t.Fatalf("install rules: code=%d body=%s", code, body)
	}
}

func (ti *testInstrument) get(t *testing.T, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(ti.control.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// startProxy starts p on a test context and shuts it down on cleanup.
func startProxy(t *testing.T, cfg Config) *Proxy {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := New(cfg)
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("proxy start: %v", err)
	}
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = p.Shutdown(sctx)
		cancel()
	})
	return p
}

// latencyRule is an inline latency rule at point with the given labels.
func latencyRule(point, delay string, labels map[string]string) string {
	lb := ""
	if len(labels) > 0 {
		parts := make([]string, 0, len(labels))
		for k, v := range labels {
			parts = append(parts, fmt.Sprintf("%q:%q", k, v))
		}
		lb = `"labels":{` + strings.Join(parts, ",") + `},`
	}
	return fmt.Sprintf(`[{"name":"lat","injection_point":%q,%s"mode":"inline",
		"fault":{"category":"inline","fault_type":"latency","params":{"delay":%q}}}]`, point, lb, delay)
}

// cacheBoxRule is an egress cache-box rule scoped to (exp-1, phase-1) with
// the canonical_v2 strategy — the same shape serve_test.go uses.
func cacheBoxRule(mode string) string {
	return fmt.Sprintf(`[{"name":"cachebox-%s","injection_point":"egress","mode":"inline",
		"cachebox":{"mode":%q,"context":{"experiment_id":"exp-1","phase_id":"phase-1",
		"key_strategy":"canonical_v2","strategy_version":2}}}]`, mode, mode)
}

func hostPort(ts *httptest.Server) string { return strings.TrimPrefix(ts.URL, "http://") }

func doGet(t *testing.T, client *http.Client, url string, header http.Header) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

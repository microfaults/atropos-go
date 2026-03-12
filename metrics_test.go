package atropos

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func gatherCounter(name string, labels map[string]string) float64 {
	families, _ := atroposRegistry.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m.GetLabel(), labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func gatherHistogramCount(name string, labels map[string]string) uint64 {
	families, _ := atroposRegistry.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m.GetLabel(), labels) {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func matchLabels(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, p := range pairs {
		if v, ok := want[p.GetName()]; !ok || v != p.GetValue() {
			return false
		}
	}
	return true
}

func TestIngressMetrics_200(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	labels := map[string]string{"method": "GET", "status_code": "200", "service": "test-ingress"}
	beforeCount := gatherCounter("http_server_requests_total", labels)
	beforeHist := gatherHistogramCount("http_server_request_duration_seconds", labels)

	mw := IngressMiddleware(handler, "test-ingress")
	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	afterCount := gatherCounter("http_server_requests_total", labels)
	afterHist := gatherHistogramCount("http_server_request_duration_seconds", labels)

	if delta := afterCount - beforeCount; delta != 1 {
		t.Fatalf("expected counter delta 1, got %v", delta)
	}
	if delta := afterHist - beforeHist; delta != 1 {
		t.Fatalf("expected histogram observation delta 1, got %v", delta)
	}
}

func TestIngressMetrics_500(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	labels := map[string]string{"method": "POST", "status_code": "500", "service": "test-500"}
	beforeCount := gatherCounter("http_server_requests_total", labels)

	mw := IngressMiddleware(handler, "test-500")
	req := httptest.NewRequest("POST", "/fail", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	afterCount := gatherCounter("http_server_requests_total", labels)
	if delta := afterCount - beforeCount; delta != 1 {
		t.Fatalf("expected counter delta 1, got %v", delta)
	}
}

func TestEgressMetrics_200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Extract host from test server URL for label matching.
	host := strings.TrimPrefix(ts.URL, "http://")
	labels := map[string]string{"method": "GET", "status_code": "200", "target": host}
	beforeCount := gatherCounter("http_client_requests_total", labels)
	beforeHist := gatherHistogramCount("http_client_request_duration_seconds", labels)

	transport := EgressTransport(http.DefaultTransport)
	client := &http.Client{Transport: transport}
	resp, err := client.Get(ts.URL + "/ok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	afterCount := gatherCounter("http_client_requests_total", labels)
	afterHist := gatherHistogramCount("http_client_request_duration_seconds", labels)

	if delta := afterCount - beforeCount; delta != 1 {
		t.Fatalf("expected counter delta 1, got %v", delta)
	}
	if delta := afterHist - beforeHist; delta != 1 {
		t.Fatalf("expected histogram observation delta 1, got %v", delta)
	}
}

func TestEgressMetrics_503(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	host := strings.TrimPrefix(ts.URL, "http://")
	labels := map[string]string{"method": "GET", "status_code": "503", "target": host}
	beforeCount := gatherCounter("http_client_requests_total", labels)

	transport := EgressTransport(http.DefaultTransport)
	client := &http.Client{Transport: transport}
	resp, err := client.Get(ts.URL + "/unavail")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	afterCount := gatherCounter("http_client_requests_total", labels)
	if delta := afterCount - beforeCount; delta != 1 {
		t.Fatalf("expected counter delta 1, got %v", delta)
	}
}

func TestEgressMetrics_TransportError(t *testing.T) {
	labels := map[string]string{"method": "GET", "status_code": "error", "target": "127.0.0.1:1"}
	beforeCount := gatherCounter("http_client_requests_total", labels)

	transport := EgressTransport(http.DefaultTransport)
	client := &http.Client{Transport: transport}
	// Port 1 is almost certainly not listening — expect a dial error.
	resp, _ := client.Get("http://127.0.0.1:1/nope")
	if resp != nil {
		resp.Body.Close()
	}

	afterCount := gatherCounter("http_client_requests_total", labels)
	if delta := afterCount - beforeCount; delta != 1 {
		t.Fatalf("expected counter delta 1 for error status, got %v", delta)
	}
}

func TestMetricsHandler_ServesMetrics(t *testing.T) {
	// Trigger at least one ingress metric so there's something to scrape.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := IngressMiddleware(handler, "handler-test")
	mw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	// Scrape via MetricsHandler.
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rec, req)

	body, _ := io.ReadAll(rec.Body)
	text := string(body)

	if !strings.Contains(text, "http_server_requests_total") {
		t.Fatal("expected http_server_requests_total in metrics output")
	}
	if !strings.Contains(text, "http_server_request_duration_seconds") {
		t.Fatal("expected http_server_request_duration_seconds in metrics output")
	}
}

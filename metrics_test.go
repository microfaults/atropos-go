package atropos

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestIngressMetrics(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		statusCode int
		service    string
		checkHist  bool
	}{
		{"200", "GET", 200, "test-ingress", true},
		{"500", "POST", 500, "test-500", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			})
			labels := map[string]string{"method": tt.method, "status_code": strconv.Itoa(tt.statusCode), "service": tt.service}
			beforeCount := gatherCounter("http_server_requests_total", labels)
			beforeHist := gatherHistogramCount("http_server_request_duration_seconds", labels)

			mw := IngressMiddleware(handler, tt.service)
			mw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tt.method, "/test", nil))

			if delta := gatherCounter("http_server_requests_total", labels) - beforeCount; delta != 1 {
				t.Fatalf("expected counter delta 1, got %v", delta)
			}
			if tt.checkHist {
				if delta := gatherHistogramCount("http_server_request_duration_seconds", labels) - beforeHist; delta != 1 {
					t.Fatalf("expected histogram delta 1, got %v", delta)
				}
			}
		})
	}
}

func TestEgressMetrics(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		checkHist  bool
	}{
		{"200", 200, true},
		{"503", 503, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			}))
			defer ts.Close()

			host := strings.TrimPrefix(ts.URL, "http://")
			labels := map[string]string{"method": "GET", "status_code": strconv.Itoa(tt.statusCode), "target": host}
			beforeCount := gatherCounter("http_client_requests_total", labels)
			beforeHist := gatherHistogramCount("http_client_request_duration_seconds", labels)

			transport := EgressTransport(http.DefaultTransport)
			client := &http.Client{Transport: transport}
			resp, err := client.Get(ts.URL + "/test")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			resp.Body.Close()

			if delta := gatherCounter("http_client_requests_total", labels) - beforeCount; delta != 1 {
				t.Fatalf("expected counter delta 1, got %v", delta)
			}
			if tt.checkHist {
				if delta := gatherHistogramCount("http_client_request_duration_seconds", labels) - beforeHist; delta != 1 {
					t.Fatalf("expected histogram delta 1, got %v", delta)
				}
			}
		})
	}
}

func TestEgressMetrics_TransportError(t *testing.T) {
	labels := map[string]string{"method": "GET", "status_code": "error", "target": "127.0.0.1:1"}
	beforeCount := gatherCounter("http_client_requests_total", labels)

	transport := EgressTransport(http.DefaultTransport)
	client := &http.Client{Transport: transport}
	resp, _ := client.Get("http://127.0.0.1:1/nope")
	if resp != nil {
		resp.Body.Close()
	}

	if delta := gatherCounter("http_client_requests_total", labels) - beforeCount; delta != 1 {
		t.Fatalf("expected counter delta 1 for error status, got %v", delta)
	}
}

func TestMetricsHandler_ServesMetrics(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := IngressMiddleware(handler, "handler-test")
	mw.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	rec := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body, _ := io.ReadAll(rec.Body)
	text := string(body)
	for _, metric := range []string{"http_server_requests_total", "http_server_request_duration_seconds"} {
		if !strings.Contains(text, metric) {
			t.Fatalf("expected %s in metrics output", metric)
		}
	}
}

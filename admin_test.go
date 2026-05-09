package atropos

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFaultAdmin_PostCPUStress(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	body := `{"category":"resource","type":"cpu","config":{"duration":"5s","target_load":0.7}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}
	if len(eval.Active()) == 0 {
		t.Fatal("expected active fault after POST")
	}
}

func TestFaultAdmin_PostNetworkLatency(t *testing.T) {
	eval := &DemoEvaluator{}
	resolver := func(target string) (string, string, error) {
		return ":19099", "localhost:6379", nil
	}
	handler := FaultAdminHandlerWith(eval, resolver)

	body := `{"category":"network","type":"latency","config":{"target":"redis","delay":"100ms","duration":"5s"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}
}

func TestFaultAdmin_PostLatency(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	// POST latency fault
	body := `{"type":"latency","config":{"delay":"200ms","jitter":"50ms"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var status FaultStatus
	json.NewDecoder(rec.Body).Decode(&status)
	if !status.Active {
		t.Fatal("expected active=true")
	}
	if status.Faults[0].Type != "latency" {
		t.Fatalf("expected type=latency, got %s", status.Faults[0].Type)
	}

	// GET should show active
	req = httptest.NewRequest(http.MethodGet, "/admin/fault", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	json.NewDecoder(rec.Body).Decode(&status)
	if !status.Active {
		t.Fatal("expected GET to show active=true")
	}

	// DELETE should clear
	req = httptest.NewRequest(http.MethodDelete, "/admin/fault", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	json.NewDecoder(rec.Body).Decode(&status)
	if status.Active {
		t.Fatal("expected active=false after DELETE")
	}

	// GET should now show inactive
	req = httptest.NewRequest(http.MethodGet, "/admin/fault", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	json.NewDecoder(rec.Body).Decode(&status)
	if status.Active {
		t.Fatal("expected GET to show active=false after DELETE")
	}
}

func TestFaultAdmin_PostError(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	body := `{"type":"error","config":{"status_code":503,"message":"service down"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var status FaultStatus
	json.NewDecoder(rec.Body).Decode(&status)
	if len(status.Faults) == 0 || !strings.Contains(string(status.Faults[0].Config), "503") {
		t.Fatalf("expected status_code=503 in config, got %s", string(status.Faults[0].Config))
	}
}

func TestFaultAdmin_PostHang(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	body := `{"type":"hang","duration":"2s"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestFaultAdmin_InvalidType(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	body := `{"type":"explode"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestFaultAdmin_MissingDelay(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	body := `{"type":"latency"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing delay, got %d", rec.Code)
	}
}

func TestFaultAdmin_InvalidJSON(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader("{bad"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad json, got %d", rec.Code)
	}
}

func TestFaultAdmin_MethodNotAllowed(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	req := httptest.NewRequest(http.MethodPut, "/admin/fault", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

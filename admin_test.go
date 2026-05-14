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

func TestFaultAdmin_MultiSlotIDs(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	// POST first inline latency with ID
	body1 := `{"id":"f1","type":"latency","config":{"delay":"100ms"}}`
	req1 := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body1))
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("failed to create f1: %s", rec1.Body.String())
	}

	// POST second inline latency with different ID
	body2 := `{"id":"f2","type":"latency","config":{"delay":"200ms"}}`
	req2 := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body2))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusCreated {
		t.Fatalf("failed to create f2: %s", rec2.Body.String())
	}

	// Should have 2 active faults
	active := eval.Active()
	if len(active) != 2 {
		t.Fatalf("expected 2 active faults, got %d", len(active))
	}

	// DELETE f1
	reqD := httptest.NewRequest(http.MethodDelete, "/admin/fault/f1", nil)
	recD := httptest.NewRecorder()
	handler.ServeHTTP(recD, reqD)
	if recD.Code != http.StatusOK {
		t.Fatalf("failed to delete f1: %d", recD.Code)
	}

	// Should have 1 active fault (f2)
	active = eval.Active()
	if len(active) != 1 {
		t.Fatalf("expected 1 active fault, got %d", len(active))
	}
	if active[0].ID != "f2" {
		t.Fatalf("expected f2 to remain, got %s", active[0].ID)
	}

	// DELETE all
	reqD2 := httptest.NewRequest(http.MethodDelete, "/admin/fault", nil)
	recD2 := httptest.NewRecorder()
	handler.ServeHTTP(recD2, reqD2)
	if len(eval.Active()) != 0 {
		t.Fatal("expected no active faults after global DELETE")
	}
}

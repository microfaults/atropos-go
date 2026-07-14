package atropos

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

func TestFaultAdmin_PostCPUStress(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	body := `{"category":"resource","fault_type":"cpu","duration_ms":5000,"params":{"target_load":0.7}}`
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
	eval := &demoEvaluator{}
	resolver := func(target string) (string, string, error) {
		return ":19099", "localhost:6379", nil
	}
	handler := faultAdminHandler(eval, resolver)

	body := `{"category":"network","fault_type":"latency","duration_ms":5000,"network":{"target":"redis"},"params":{"delay":"100ms"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}
}

func TestFaultAdmin_PostLatency(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	// POST latency fault
	body := `{"fault_type":"latency","params":{"delay":"200ms","jitter":"50ms"}}`
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
	if status.Faults[0].FaultType != "latency" {
		t.Fatalf("expected fault_type=latency, got %s", status.Faults[0].FaultType)
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
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	body := `{"fault_type":"error","params":{"status_code":503,"message":"service down"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var status FaultStatus
	json.NewDecoder(rec.Body).Decode(&status)
	if len(status.Faults) == 0 || !strings.Contains(string(status.Faults[0].Params), "503") {
		t.Fatalf("expected status_code=503 in params, got %s", string(status.Faults[0].Params))
	}
}

func TestFaultAdmin_PostHang(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	body := `{"fault_type":"hang","params":{"duration":"2s"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestFaultAdmin_InvalidType(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	body := `{"fault_type":"explode"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestFaultAdmin_MissingDelay(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	body := `{"fault_type":"latency"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing delay, got %d", rec.Code)
	}
}

func TestFaultAdmin_InvalidJSON(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	req := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader("{bad"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad json, got %d", rec.Code)
	}
}

func TestFaultAdmin_MethodNotAllowed(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	req := httptest.NewRequest(http.MethodPut, "/admin/fault", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// seedLongFault returns a startFn for a background-style fault that runs
// until its context is cancelled, mirroring how rule-attached background
// faults land in the registry.
func seedLongFault(ctx context.Context) (*fault.Handle, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	h := fault.NewHandle(cancel)
	go func() {
		<-ctx.Done()
		h.Send(fault.Result{})
	}()
	return h, nil
}

// TestAdminFault_ComposesWithHostEvaluator_RegistryUntouched pins the A1
// successor guarantee, now structural: the admin fault slot is a SECOND
// evaluator composed after the host's — arming and clearing admin faults can
// neither replace the host evaluator (host rules keep winning where they
// match) nor swap/close the process-lifetime fault registry. The lazily
// reconfiguring zero-arg handler this used to guard against no longer
// exists.
func TestAdminFault_ComposesWithHostEvaluator_RegistryUntouched(t *testing.T) {
	host := evaluator.NewStaticEvaluator(StaticRule{
		Name:  "host-egress-rule",
		Point: evaluator.Egress,
		Decision: evaluator.Decision{
			Name:     "host-egress-rule",
			CacheBox: evaluator.CacheBoxReplay,
			Reason:   "compiled",
		},
	})
	demo := &demoEvaluator{}
	configure(evaluator.NewMultiEvaluator(host, demo), nil)
	defer configure(nil, nil) // reset the package interceptor for later tests

	regBefore := defaultRegistry

	// Seed a running background fault through the shared registry, as a
	// rule-attached fault would.
	h, deduped, err := defaultRegistry.StartOrJoin("host-bg", evaluator.DeduplicateByRule, seedLongFault)
	if err != nil || deduped {
		t.Fatalf("seed fault: err=%v deduped=%v", err, deduped)
	}

	// Arm an admin fault through the handler.
	handler := faultAdminHandler(demo, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/fault",
		strings.NewReader(`{"fault_type":"latency","params":{"delay":"50ms"}}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("arm admin fault: %d %s", rec.Code, rec.Body.String())
	}

	// (a) Where a host rule matches, it wins: the armed admin latency must
	// not shadow the host's egress replay rule mid-experiment.
	d := currentInterceptor().Evaluate(context.Background(), evaluator.Request{Point: evaluator.Egress})
	if d == nil || d.Name != "host-egress-rule" {
		t.Fatalf("egress decision = %+v, want the host rule to win over the admin slot", d)
	}

	// (b) Where no host rule matches, the admin slot fills the gap.
	d = currentInterceptor().Evaluate(context.Background(), evaluator.Request{Point: evaluator.Ingress})
	if d == nil || d.Reason != "admin" {
		t.Fatalf("ingress decision = %+v, want the admin fault", d)
	}

	// (c) Arm + clear left the registry alone: same pointer, seeded fault
	// still running, and still stoppable through the same registry.
	recD := httptest.NewRecorder()
	handler.ServeHTTP(recD, httptest.NewRequest(http.MethodDelete, "/admin/fault", nil))
	if recD.Code != http.StatusOK {
		t.Fatalf("clear admin faults: %d", recD.Code)
	}
	if defaultRegistry != regBefore {
		t.Fatal("defaultRegistry pointer changed across admin arm/clear")
	}
	select {
	case <-h.Done():
		t.Fatal("admin arm/clear cancelled a host background fault")
	case <-time.After(100 * time.Millisecond):
	}
	defaultRegistry.Stop("host-bg")
	select {
	case <-h.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("seeded fault did not stop through the registry")
	}
}

func TestFaultAdmin_MultiSlotIDs(t *testing.T) {
	eval := &demoEvaluator{}
	handler := faultAdminHandler(eval, nil)

	// POST first inline latency with ID
	body1 := `{"id":"f1","fault_type":"latency","params":{"delay":"100ms"}}`
	req1 := httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body1))
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("failed to create f1: %s", rec1.Body.String())
	}

	// POST second inline latency with different ID
	body2 := `{"id":"f2","fault_type":"latency","params":{"delay":"200ms"}}`
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

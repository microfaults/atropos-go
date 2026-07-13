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
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

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
	eval := &DemoEvaluator{}
	resolver := func(target string) (string, string, error) {
		return ":19099", "localhost:6379", nil
	}
	handler := FaultAdminHandlerWith(eval, resolver)

	body := `{"category":"network","fault_type":"latency","duration_ms":5000,"network":{"target":"redis"},"params":{"delay":"100ms"}}`
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
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

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
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	body := `{"fault_type":"hang","params":{"duration":"2s"}}`
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

	body := `{"fault_type":"explode"}`
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

	body := `{"fault_type":"latency"}`
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

// TestFaultAdminHandler_HostConfigured_Returns409 pins the A1 fix: once a
// host has called Configure, a stray request to the zero-arg admin handler
// (even a GET) must NOT lazily reconfigure the SDK onto the demo evaluator.
// Doing so previously closed the shared fault registry mid-experiment,
// cancelling running background faults and orphaning the live middleware's
// interceptor. The handler must return 409 and leave the registry untouched.
func TestFaultAdminHandler_HostConfigured_Returns409(t *testing.T) {
	// Host configures the SDK with its own evaluator + cache-box (the
	// manteion-driven path). The live middleware captures this interceptor.
	eval := NewStaticEvaluator(StaticRule{Name: "r1", Point: Egress})
	cb := NewCacheBox(CacheBoxConfig{})
	Configure(WithEvaluator(eval), WithCacheBoxCoordinator(cb))
	defer Configure() // reset the package interceptor for later tests

	regBefore := defaultRegistry

	mwCalled := false
	mw := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mwCalled = true
		w.WriteHeader(http.StatusOK)
	}), "a1-svc")

	// Seed a running background fault through the shared registry.
	h, deduped, err := defaultRegistry.StartOrJoin("a1-bg", evaluator.DeduplicateByRule, seedLongFault)
	if err != nil || deduped {
		t.Fatalf("seed fault: err=%v deduped=%v", err, deduped)
	}

	// A stray GET to the zero-arg admin handler must be refused, not honored.
	rec := httptest.NewRecorder()
	FaultAdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/fault", nil))

	// (a) 409 with a JSON error envelope.
	if rec.Code != http.StatusConflict {
		t.Fatalf("GET on host-configured SDK: status = %d, want 409; body = %s", rec.Code, rec.Body.String())
	}
	var er ErrorResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &er); uerr != nil || er.Error == "" {
		t.Fatalf("expected JSON error body, got %q (unmarshal err=%v)", rec.Body.String(), uerr)
	}

	// (b) the registry was neither swapped nor closed: same pointer, and the
	// previously-started fault is still live (not cancelled by a reconfigure).
	if defaultRegistry != regBefore {
		t.Fatal("defaultRegistry pointer changed after a host-configured GET")
	}
	select {
	case <-h.Done():
		t.Fatal("host-configured GET closed the registry and cancelled a running background fault")
	case <-time.After(100 * time.Millisecond):
	}

	// (c) a fresh StartOrJoin on the same registry still succeeds (not closed).
	h2, _, err := defaultRegistry.StartOrJoin("a1-probe", evaluator.DeduplicateByRule, seedLongFault)
	if err != nil {
		t.Fatalf("registry refused a new fault after the GET (closed?): %v", err)
	}

	// The live middleware still routes through its captured interceptor.
	mwRec := httptest.NewRecorder()
	mw.ServeHTTP(mwRec, httptest.NewRequest(http.MethodGet, "/", nil))
	if mwRec.Code != http.StatusOK || !mwCalled {
		t.Fatalf("live middleware broken after GET: code=%d called=%v", mwRec.Code, mwCalled)
	}

	// The seeded fault remains stoppable through the same registry.
	defaultRegistry.Stop("a1-bg")
	select {
	case <-h.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("seeded fault did not stop through the registry")
	}

	// Clean up so the shared registry returns empty for later tests.
	defaultRegistry.Stop("a1-probe")
	<-h2.Done()
}

// TestFaultAdminHandlerWith_ExplicitMountUnaffected proves the 409 guard is
// scoped to the zero-arg handler: the explicit mount-your-own constructor
// keeps serving normally even when the SDK is host-configured.
func TestFaultAdminHandlerWith_ExplicitMountUnaffected(t *testing.T) {
	Configure(WithEvaluator(NewStaticEvaluator()))
	defer Configure()
	if !hostConfigured.Load() {
		t.Fatal("precondition: expected hostConfigured after Configure")
	}

	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

	rec := httptest.NewRecorder()
	body := `{"fault_type":"latency","params":{"delay":"100ms"}}`
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/fault", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("explicit handler POST: status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/fault", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit handler GET: status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var status FaultStatus
	json.NewDecoder(rec.Body).Decode(&status)
	if !status.Active {
		t.Fatal("expected explicit handler to report the active fault")
	}
}

func TestFaultAdmin_MultiSlotIDs(t *testing.T) {
	eval := &DemoEvaluator{}
	handler := FaultAdminHandlerWith(eval, nil)

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

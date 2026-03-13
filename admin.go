package atropos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/microfaults/atropos-go/internal/fault"
)

// DemoEvaluator is a trivial Evaluator for demos and development.
// When a fault is set, every Evaluate call returns that decision.
// When cleared, no fault is injected. Safe for concurrent use.
type DemoEvaluator struct {
	mu       sync.RWMutex
	decision *Decision
	req      *faultRequest // keep original request for GET serialization

	// Background resource fault state.
	resourceHandle *fault.Handle
	resourceReq    *faultRequest
}

// Evaluate implements Evaluator.
func (d *DemoEvaluator) Evaluate(_ context.Context, _ Request) *Decision {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.decision
}

// Set activates an inline fault. All subsequent Evaluate calls return this decision.
func (d *DemoEvaluator) Set(decision *Decision, req *faultRequest) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decision = decision
	d.req = req
}

// SetResource starts a background resource fault and tracks its handle.
func (d *DemoEvaluator) SetResource(handle *fault.Handle, req *faultRequest) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Stop any previously running resource fault.
	if d.resourceHandle != nil {
		d.resourceHandle.Stop()
	}
	d.resourceHandle = handle
	d.resourceReq = req
}

// Clear deactivates all faults (inline and resource).
func (d *DemoEvaluator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decision = nil
	d.req = nil
	if d.resourceHandle != nil {
		d.resourceHandle.Stop()
		d.resourceHandle = nil
		d.resourceReq = nil
	}
}

// Active returns the current fault request, or nil if inactive.
// Prefers inline faults; falls back to resource faults.
func (d *DemoEvaluator) Active() *faultRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.req != nil {
		return d.req
	}
	return d.resourceReq
}

// ActiveAll returns both inline and resource fault requests.
func (d *DemoEvaluator) ActiveAll() (inline *faultRequest, resource *faultRequest) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.req, d.resourceReq
}

// faultRequest is the JSON body for POST /admin/fault.
type faultRequest struct {
	Type       string  `json:"type"`                   // "latency", "error", "hang", "cpu", "memory"
	Delay      string  `json:"delay,omitempty"`         // duration string for latency
	Jitter     string  `json:"jitter,omitempty"`        // duration string for latency jitter
	Duration   string  `json:"duration,omitempty"`      // duration string for hang/cpu/memory
	StatusCode int     `json:"status_code,omitempty"`   // HTTP status for error fault
	Message    string  `json:"message,omitempty"`        // error message for error fault
	Load       float64 `json:"load,omitempty"`           // target load fraction for cpu/memory (0,1]
	RampUp     string  `json:"ramp_up,omitempty"`        // ramp-up duration for cpu/memory
	RampDown   string  `json:"ramp_down,omitempty"`      // ramp-down duration for cpu/memory
	Thrashing  bool    `json:"thrashing,omitempty"`      // enable thrashing for memory fault
}

// faultStatus is the JSON response for GET /admin/fault.
type faultStatus struct {
	Active   bool          `json:"active"`
	Fault    *faultRequest `json:"fault,omitempty"`
	Resource *faultRequest `json:"resource,omitempty"`
}

var (
	demoEval     *DemoEvaluator
	demoEvalOnce sync.Once
)

func ensureDemoEval() *DemoEvaluator {
	demoEvalOnce.Do(func() {
		demoEval = &DemoEvaluator{}
		Configure(demoEval)
	})
	return demoEval
}

// FaultAdminHandler returns an http.Handler for runtime fault control.
//
// Supported methods:
//   - POST: activate a fault (JSON body with type, delay, etc.)
//   - DELETE: deactivate the current fault
//   - GET: return the current fault status
//
// Supported fault types:
//   - "latency": inline request delay
//   - "error": inline HTTP error response
//   - "hang": inline request hang
//   - "cpu": background CPU stress (fixed duration)
//   - "memory": background memory stress (fixed duration)
//
// Example:
//
//	mux.Handle("/admin/fault", atropos.FaultAdminHandler())
//	// curl -X POST localhost:8080/admin/fault -d '{"type":"latency","delay":"500ms"}'
//	// curl -X POST localhost:8080/admin/fault -d '{"type":"cpu","duration":"30s","load":0.8}'
//	// curl -X POST localhost:8080/admin/fault -d '{"type":"memory","duration":"30s","load":0.6,"thrashing":true}'
//	// curl -X DELETE localhost:8080/admin/fault
//	// curl localhost:8080/admin/fault
func FaultAdminHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eval := ensureDemoEval()
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodPost:
			handleFaultPost(w, r, eval)
		case http.MethodDelete:
			eval.Clear()
			json.NewEncoder(w).Encode(faultStatus{Active: false})
		case http.MethodGet:
			inlineReq, resourceReq := eval.ActiveAll()
			active := inlineReq != nil || resourceReq != nil
			json.NewEncoder(w).Encode(faultStatus{
				Active:   active,
				Fault:    inlineReq,
				Resource: resourceReq,
			})
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})
}

func handleFaultPost(w http.ResponseWriter, r *http.Request, eval *DemoEvaluator) {
	var req faultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid json: %s"}`, err), http.StatusBadRequest)
		return
	}

	switch req.Type {
	case "latency":
		handleInlineLatency(w, eval, &req)
	case "error":
		handleInlineError(w, eval, &req)
	case "hang":
		handleInlineHang(w, eval, &req)
	case "cpu":
		handleResourceCPU(w, eval, &req)
	case "memory":
		handleResourceMemory(w, eval, &req)
	default:
		http.Error(w, `{"error":"unknown fault type, must be: latency, error, hang, cpu, memory"}`, http.StatusBadRequest)
	}
}

func handleInlineLatency(w http.ResponseWriter, eval *DemoEvaluator, req *faultRequest) {
	delay, err := time.ParseDuration(req.Delay)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid delay: %s"}`, err), http.StatusBadRequest)
		return
	}
	var jitter time.Duration
	if req.Jitter != "" {
		jitter, err = time.ParseDuration(req.Jitter)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid jitter: %s"}`, err), http.StatusBadRequest)
			return
		}
	}
	f := NewLatencyFault(delay, jitter)
	setInlineFault(w, eval, f, req)
}

func handleInlineError(w http.ResponseWriter, eval *DemoEvaluator, req *faultRequest) {
	if req.StatusCode == 0 {
		req.StatusCode = http.StatusInternalServerError
	}
	if req.Message == "" {
		req.Message = "injected fault"
	}
	f := NewErrorFault(req.StatusCode, req.Message)
	setInlineFault(w, eval, f, req)
}

func handleInlineHang(w http.ResponseWriter, eval *DemoEvaluator, req *faultRequest) {
	dur, err := time.ParseDuration(req.Duration)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid duration: %s"}`, err), http.StatusBadRequest)
		return
	}
	f := NewHangFault(dur)
	setInlineFault(w, eval, f, req)
}

func setInlineFault(w http.ResponseWriter, eval *DemoEvaluator, f Fault, req *faultRequest) {
	decision := &Decision{
		Fault:  f,
		Reason: "admin",
		Mode:   Inline,
	}
	eval.Set(decision, req)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(faultStatus{Active: true, Fault: req})
}

// parseResourceCommon extracts duration, load, ramp_up, ramp_down from a faultRequest.
func parseResourceCommon(req *faultRequest) (dur, rampUp, rampDown time.Duration, load float64, err error) {
	if req.Duration == "" {
		err = fmt.Errorf("duration is required for %s fault", req.Type)
		return
	}
	dur, err = time.ParseDuration(req.Duration)
	if err != nil {
		err = fmt.Errorf("invalid duration: %w", err)
		return
	}

	load = req.Load
	if load <= 0 || load > 1.0 {
		err = fmt.Errorf("load must be in (0.0, 1.0], got %s", strconv.FormatFloat(load, 'f', -1, 64))
		return
	}

	if req.RampUp != "" {
		rampUp, err = time.ParseDuration(req.RampUp)
		if err != nil {
			err = fmt.Errorf("invalid ramp_up: %w", err)
			return
		}
	}
	if req.RampDown != "" {
		rampDown, err = time.ParseDuration(req.RampDown)
		if err != nil {
			err = fmt.Errorf("invalid ramp_down: %w", err)
			return
		}
	}
	return
}

func handleResourceCPU(w http.ResponseWriter, eval *DemoEvaluator, req *faultRequest) {
	dur, rampUp, rampDown, load, err := parseResourceCommon(req)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusBadRequest)
		return
	}

	stress := NewCPUStressFault(CPUStressOpts{
		Duration: dur,
		Load:     load,
		RampUp:   rampUp,
		RampDown: rampDown,
	})

	handle, err := stress.Start(context.Background())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to start cpu fault: %s"}`, err), http.StatusInternalServerError)
		return
	}

	eval.SetResource(handle, req)

	// Auto-clear when the fault finishes naturally.
	go func() {
		<-handle.Done()
		eval.mu.Lock()
		defer eval.mu.Unlock()
		if eval.resourceHandle == handle {
			eval.resourceHandle = nil
			eval.resourceReq = nil
		}
	}()

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(faultStatus{Active: true, Resource: req})
}

func handleResourceMemory(w http.ResponseWriter, eval *DemoEvaluator, req *faultRequest) {
	dur, rampUp, rampDown, load, err := parseResourceCommon(req)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusBadRequest)
		return
	}

	stress := NewMemoryStressFault(MemoryStressOpts{
		Duration:  dur,
		Load:      load,
		RampUp:    rampUp,
		RampDown:  rampDown,
		Thrashing: req.Thrashing,
	})

	handle, err := stress.Start(context.Background())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to start memory fault: %s"}`, err), http.StatusInternalServerError)
		return
	}

	eval.SetResource(handle, req)

	// Auto-clear when the fault finishes naturally.
	go func() {
		<-handle.Done()
		eval.mu.Lock()
		defer eval.mu.Unlock()
		if eval.resourceHandle == handle {
			eval.resourceHandle = nil
			eval.resourceReq = nil
		}
	}()

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(faultStatus{Active: true, Resource: req})
}

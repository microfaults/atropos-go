package atropos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// DemoEvaluator is a trivial Evaluator for demos and development.
// When a fault is set, every Evaluate call returns that decision.
// When cleared, no fault is injected. Safe for concurrent use.
type DemoEvaluator struct {
	mu       sync.RWMutex
	decision *Decision
	req      *faultRequest // keep original request for GET serialization
}

// Evaluate implements Evaluator.
func (d *DemoEvaluator) Evaluate(_ context.Context, _ Request) *Decision {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.decision
}

// Set activates a fault. All subsequent Evaluate calls return this decision.
func (d *DemoEvaluator) Set(decision *Decision, req *faultRequest) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decision = decision
	d.req = req
}

// Clear deactivates the fault. Evaluate returns nil after this call.
func (d *DemoEvaluator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decision = nil
	d.req = nil
}

// Active returns the current fault request, or nil if inactive.
func (d *DemoEvaluator) Active() *faultRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.req
}

// faultRequest is the JSON body for POST /admin/fault.
type faultRequest struct {
	Type       string `json:"type"`                  // "latency", "error", "hang"
	Delay      string `json:"delay,omitempty"`        // duration string for latency
	Jitter     string `json:"jitter,omitempty"`       // duration string for latency jitter
	Duration   string `json:"duration,omitempty"`     // duration string for hang
	StatusCode int    `json:"status_code,omitempty"`  // HTTP status for error fault
	Message    string `json:"message,omitempty"`      // error message for error fault
}

// faultStatus is the JSON response for GET /admin/fault.
type faultStatus struct {
	Active bool          `json:"active"`
	Fault  *faultRequest `json:"fault,omitempty"`
}

var (
	demoEval     *DemoEvaluator
	demoEvalOnce sync.Once
)

func ensureDemoEval() *DemoEvaluator {
	demoEvalOnce.Do(func() {
		demoEval = &DemoEvaluator{}
		Configure(WithEvaluator(demoEval))
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
// Example:
//
//	mux.Handle("/admin/fault", atropos.FaultAdminHandler())
//	// curl -X POST http://localhost:8080/admin/fault -d '{"type":"latency","delay":"500ms"}'
//	// curl -X DELETE http://localhost:8080/admin/fault
//	// curl http://localhost:8080/admin/fault
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
			req := eval.Active()
			if req != nil {
				json.NewEncoder(w).Encode(faultStatus{Active: true, Fault: req})
			} else {
				json.NewEncoder(w).Encode(faultStatus{Active: false})
			}
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

	var f Fault
	switch req.Type {
	case "latency":
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
		f = NewLatencyFault(delay, jitter)

	case "error":
		if req.StatusCode == 0 {
			req.StatusCode = http.StatusInternalServerError
		}
		if req.Message == "" {
			req.Message = "injected fault"
		}
		f = NewErrorFault(req.StatusCode, req.Message)

	case "hang":
		dur, err := time.ParseDuration(req.Duration)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid duration: %s"}`, err), http.StatusBadRequest)
			return
		}
		f = NewHangFault(dur)

	default:
		http.Error(w, `{"error":"unknown fault type, must be: latency, error, hang"}`, http.StatusBadRequest)
		return
	}

	decision := &Decision{
		Fault:  f,
		Reason: "admin",
		Mode:   Inline,
	}
	eval.Set(decision, &req)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(faultStatus{Active: true, Fault: &req})
}

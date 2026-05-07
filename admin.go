package atropos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// DemoEvaluator is a trivial Evaluator for demos and development.
// When a fault is set, every Evaluate call returns that decision.
// When cleared, no fault is injected. Safe for concurrent use.
type DemoEvaluator struct {
	mu       sync.RWMutex
	decision *Decision
	req      *FaultRequest // keep original request for GET serialization
}

// Evaluate implements Evaluator.
func (d *DemoEvaluator) Evaluate(_ context.Context, _ Request) *Decision {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.decision
}

// Set activates a fault. All subsequent Evaluate calls return this decision.
func (d *DemoEvaluator) Set(decision *Decision, req *FaultRequest) {
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
func (d *DemoEvaluator) Active() *FaultRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.req
}

// FaultRequest is the JSON body for POST /admin/fault.
type FaultRequest struct {
	Category   string          `json:"category,omitempty"`    // "inline" (default), "network", "resource"
	Type       string          `json:"type"`                  // fault type within category
	Delay      string          `json:"delay,omitempty"`       // duration string for inline latency
	Jitter     string          `json:"jitter,omitempty"`      // duration string for inline latency jitter
	Duration   string          `json:"duration,omitempty"`    // duration string for inline hang
	StatusCode int             `json:"status_code,omitempty"` // HTTP status for inline error fault
	Message    string          `json:"message,omitempty"`     // error message for inline error fault
	Config     json.RawMessage `json:"config,omitempty"`      // extended config for network/resource
}

func (r *FaultRequest) effectiveCategory() string {
	if r.Category == "" {
		return "inline"
	}
	return r.Category
}

// FaultStatus is the JSON response for GET /admin/fault.
type FaultStatus struct {
	Active bool          `json:"active"`
	Fault  *FaultRequest `json:"fault,omitempty"`
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
		FaultAdminHandlerWith(eval, nil).ServeHTTP(w, r)
	})
}

// FaultAdminHandlerWith returns an http.Handler wired to the given evaluator
// and optional NetworkResolver. Use this constructor when the admin endpoint
// needs to accept network-category faults.
func FaultAdminHandlerWith(eval *DemoEvaluator, resolve NetworkResolver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			handleFaultPost(w, r, eval, resolve)
		case http.MethodDelete:
			eval.Clear()
			json.NewEncoder(w).Encode(FaultStatus{Active: false})
		case http.MethodGet:
			req := eval.Active()
			if req != nil {
				json.NewEncoder(w).Encode(FaultStatus{Active: true, Fault: req})
			} else {
				json.NewEncoder(w).Encode(FaultStatus{Active: false})
			}
		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func handleFaultPost(w http.ResponseWriter, r *http.Request, eval *DemoEvaluator, resolve NetworkResolver) {
	var req FaultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}

	f, err := buildFault(req, resolve)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	mode := Inline
	if req.effectiveCategory() != "inline" {
		mode = Background
	}

	decision := &Decision{
		Name:   "admin",
		Fault:  f,
		Reason: "admin",
		Mode:   mode,
	}
	eval.Set(decision, &req)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(FaultStatus{Active: true, Fault: &req})
}

// buildFault dispatches fault construction by category. Shared between
// admin.go (handleFaultPost) and register.go (applyActiveFault).
func buildFault(req FaultRequest, resolve NetworkResolver) (Fault, error) {
	switch req.effectiveCategory() {
	case "inline":
		return buildInlineFault(req)
	case "network":
		return buildNetworkFault(req, resolve)
	case "resource":
		return buildResourceFault(req)
	default:
		return nil, fmt.Errorf("unknown category %q", req.Category)
	}
}

func buildInlineFault(req FaultRequest) (Fault, error) {
	switch req.Type {
	case "latency":
		delay, err := time.ParseDuration(req.Delay)
		if err != nil {
			return nil, fmt.Errorf("invalid delay %q: %w", req.Delay, err)
		}
		var jitter time.Duration
		if req.Jitter != "" {
			jitter, err = time.ParseDuration(req.Jitter)
			if err != nil {
				return nil, fmt.Errorf("invalid jitter %q: %w", req.Jitter, err)
			}
		}
		return NewLatencyFault(delay, jitter), nil
	case "error":
		status := req.StatusCode
		if status == 0 {
			status = http.StatusInternalServerError
		}
		msg := req.Message
		if msg == "" {
			msg = "injected fault"
		}
		return NewErrorFault(status, msg), nil
	case "hang":
		dur, err := time.ParseDuration(req.Duration)
		if err != nil {
			return nil, fmt.Errorf("invalid hang duration %q: %w", req.Duration, err)
		}
		return NewHangFault(dur), nil
	default:
		return nil, fmt.Errorf("unknown inline fault type %q", req.Type)
	}
}

func buildNetworkFault(req FaultRequest, resolve NetworkResolver) (Fault, error) {
	if resolve == nil {
		return nil, fmt.Errorf("network fault %q requires a NetworkResolver", req.Type)
	}
	cfg := req.Config
	if cfg == nil {
		cfg = json.RawMessage(`{}`)
	}
	var envelope struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(cfg, &envelope); err != nil {
		return nil, fmt.Errorf("decode network config: %w", err)
	}
	dur, err := time.ParseDuration(envelope.Duration)
	if err != nil {
		return nil, fmt.Errorf("invalid network duration %q: %w", envelope.Duration, err)
	}
	baseCfg := fault.FaultConfig{Duration: dur}
	return decodeNetworkFault(req.Type, cfg, baseCfg, resolve)
}

func buildResourceFault(req FaultRequest) (Fault, error) {
	cfg := req.Config
	if cfg == nil {
		cfg = json.RawMessage(`{}`)
	}
	var envelope struct {
		Duration string `json:"duration"`
		RampUp   string `json:"ramp_up"`
		RampDown string `json:"ramp_down"`
	}
	if err := json.Unmarshal(cfg, &envelope); err != nil {
		return nil, fmt.Errorf("decode resource config: %w", err)
	}
	dur, err := time.ParseDuration(envelope.Duration)
	if err != nil {
		return nil, fmt.Errorf("invalid resource duration %q: %w", envelope.Duration, err)
	}
	baseCfg := fault.FaultConfig{Duration: dur}
	if envelope.RampUp != "" {
		baseCfg.RampUp, err = time.ParseDuration(envelope.RampUp)
		if err != nil {
			return nil, fmt.Errorf("invalid ramp_up %q: %w", envelope.RampUp, err)
		}
	}
	if envelope.RampDown != "" {
		baseCfg.RampDown, err = time.ParseDuration(envelope.RampDown)
		if err != nil {
			return nil, fmt.Errorf("invalid ramp_down %q: %w", envelope.RampDown, err)
		}
	}
	return decodeResourceFault(req.Type, cfg, baseCfg)
}

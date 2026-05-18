package atropos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

type faultSlot struct {
	decision        *Decision
	req             *FaultRequest
	lastConfirmedAt time.Time
}

type DemoEvaluator struct {
	mu    sync.RWMutex
	slots map[string]*faultSlot // key = ID (service+category:type)
}

// Evaluate returns the first decision matching the request, in
// inline > network > resource priority. When multiple slots share a category,
// they are visited in lexicographic ID order so the chosen decision is stable
// across calls.
//
// Limitation: this returns a single decision per call, so only one armed slot
// is ever effective at a time. Slots in lower-priority categories or with
// higher-sorting IDs are inert as long as a winning slot exists. See
// docs/plans/2026-05-17-concurrent-multi-fault-execution.md for the path to
// true concurrent multi-fault execution.
func (d *DemoEvaluator) Evaluate(_ context.Context, _ Request) *Decision {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ids := d.sortedIDsLocked()
	for _, cat := range []string{"inline", "network", "resource"} {
		for _, id := range ids {
			slot := d.slots[id]
			if slot.req.effectiveCategory() == cat && slot.decision != nil {
				return slot.decision
			}
		}
	}
	return nil
}

// sortedIDsLocked returns slot IDs in lexicographic order. Caller must hold d.mu.
func (d *DemoEvaluator) sortedIDsLocked() []string {
	ids := make([]string, 0, len(d.slots))
	for id := range d.slots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Set installs or replaces the slot for req.ID.
// If req.ID is empty, it falls back to effectiveCategory.
func (d *DemoEvaluator) Set(decision *Decision, req *FaultRequest) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.slots == nil {
		d.slots = make(map[string]*faultSlot)
	}

	id := req.ID
	if id == "" {
		id = req.effectiveCategory()
	}

	d.slots[id] = &faultSlot{
		decision:        decision,
		req:             req,
		lastConfirmedAt: time.Now(),
	}
}

// ClearSlot deletes the fault slot with the given ID. ID matches the key used
// by Set (req.ID, or effectiveCategory when req.ID is empty).
func (d *DemoEvaluator) ClearSlot(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.slots, id)
}

// Clear deactivates all faults.
func (d *DemoEvaluator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.slots = make(map[string]*faultSlot)
}

// Confirm bumps lastConfirmedAt to now for the given ID.
func (d *DemoEvaluator) Confirm(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s, ok := d.slots[id]; ok {
		s.lastConfirmedAt = time.Now()
	}
}

// ActiveIDs returns the IDs of all currently-armed slots, in lexicographic order.
func (d *DemoEvaluator) ActiveIDs() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sortedIDsLocked()
}

// StaleSlots returns IDs whose lastConfirmedAt is older than maxAge.
func (d *DemoEvaluator) StaleSlots(maxAge time.Duration) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	cutoff := time.Now().Add(-maxAge)
	var stale []string
	for id, s := range d.slots {
		if s.lastConfirmedAt.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	return stale
}

// Active returns all active fault requests, grouped by category in
// inline > network > resource order and lexicographic by ID within each group.
func (d *DemoEvaluator) Active() []*FaultRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ids := d.sortedIDsLocked()
	out := make([]*FaultRequest, 0, len(ids))

	for _, cat := range []string{"inline", "network", "resource"} {
		for _, id := range ids {
			slot := d.slots[id]
			if slot.req != nil && slot.req.effectiveCategory() == cat {
				out = append(out, slot.req)
			}
		}
	}
	return out
}

type FaultRequest struct {
	ID         string          `json:"id,omitempty"`     // unique identifier (service+category:type)
	Category   string          `json:"category"`         // "inline" (default), "network", "resource"
	Type       string          `json:"type"`             // fault type within category
	DurationMs int64           `json:"duration_ms"`      // 0 = infinite (whitelist enforced server-side)
	Config     json.RawMessage `json:"config,omitempty"` // extended config for network/resource/inline
}

func (r *FaultRequest) effectiveCategory() string {
	if r.Category == "" {
		return "inline"
	}
	return r.Category
}

// FaultStatus is the JSON response for GET /admin/fault.
type FaultStatus struct {
	Active bool            `json:"active"`
	Faults []*FaultRequest `json:"faults,omitempty"` // matches new plan shape {"faults": [...]}
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
//
// Interaction with Manteion: admin POSTs key the slot by req.ID (or the
// effectiveCategory when ID is empty). When a ManteionClient is also running,
// every successful poll runs reconciliation in Apply, which drops slot IDs not
// present in the server's active_faults response. And because admin POST never
// calls Confirm, the fault watchdog will reap admin slots after the grace
// period (max(3*pollInterval, 30s)). Treat admin faults as short-lived overrides
// when connected to Manteion; in offline mode they persist until DELETEd.
func FaultAdminHandlerWith(eval *DemoEvaluator, resolve NetworkResolver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Route manually to support DELETE /admin/fault/{category}
		path := r.URL.Path

		switch r.Method {
		case http.MethodPost:
			handleFaultPost(w, r, eval, resolve)
		case http.MethodDelete:
			// check if path is /admin/fault or /admin/fault/{category}
			if path == "/admin/fault" || path == "/admin/fault/" {
				eval.Clear()
			} else {
				// strip /admin/fault/ to get ID
				id := path[len("/admin/fault/"):]
				eval.ClearSlot(id)
			}
			json.NewEncoder(w).Encode(FaultStatus{Active: false})
		case http.MethodGet:
			reqs := eval.Active()
			if len(reqs) > 0 {
				json.NewEncoder(w).Encode(FaultStatus{Active: true, Faults: reqs})
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
	json.NewEncoder(w).Encode(FaultStatus{Active: true, Faults: []*FaultRequest{&req}})
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
	cfg := req.Config
	if cfg == nil {
		cfg = json.RawMessage(`{}`)
	}

	switch req.Type {
	case "latency":
		var env struct {
			Delay  string `json:"delay"`
			Jitter string `json:"jitter"`
		}
		if err := json.Unmarshal(cfg, &env); err != nil {
			return nil, fmt.Errorf("decode latency config: %w", err)
		}
		delay, err := time.ParseDuration(env.Delay)
		if err != nil {
			return nil, fmt.Errorf("invalid delay %q: %w", env.Delay, err)
		}
		var jitter time.Duration
		if env.Jitter != "" {
			jitter, err = time.ParseDuration(env.Jitter)
			if err != nil {
				return nil, fmt.Errorf("invalid jitter %q: %w", env.Jitter, err)
			}
		}
		return NewLatencyFault(delay, jitter), nil
	case "error":
		var env struct {
			StatusCode int    `json:"status_code"`
			Message    string `json:"message"`
		}
		if err := json.Unmarshal(cfg, &env); err != nil {
			return nil, fmt.Errorf("decode error config: %w", err)
		}
		status := env.StatusCode
		if status == 0 {
			status = http.StatusInternalServerError
		}
		msg := env.Message
		if msg == "" {
			msg = "injected fault"
		}
		return NewErrorFault(status, msg), nil
	case "hang":
		var env struct {
			Duration string `json:"duration"`
		}
		if err := json.Unmarshal(cfg, &env); err != nil {
			return nil, fmt.Errorf("decode hang config: %w", err)
		}
		dur, err := time.ParseDuration(env.Duration)
		if err != nil {
			return nil, fmt.Errorf("invalid hang duration %q: %w", env.Duration, err)
		}
		return NewHangFault(dur), nil
	default:
		return nil, fmt.Errorf("unknown inline fault type %q", req.Type)
	}
}

func buildNetworkFault(req FaultRequest, resolve NetworkResolver) (Fault, error) {
	cfg := req.Config
	if cfg == nil {
		cfg = json.RawMessage(`{}`)
	}
	// Admin wire format keeps envelope and params merged inside Config for
	// CLI ergonomics. Split them here to match the new decodeNetworkFault
	// envelope-based signature.
	var merged struct {
		Host      string  `json:"host"`
		Target    string  `json:"target"`
		Direction string  `json:"direction"`
		Scope     float64 `json:"scope"`
		Duration  string  `json:"duration"`
	}
	if err := json.Unmarshal(cfg, &merged); err != nil {
		return nil, fmt.Errorf("decode network config: %w", err)
	}
	dur, err := time.ParseDuration(merged.Duration)
	if err != nil {
		return nil, fmt.Errorf("invalid network duration %q: %w", merged.Duration, err)
	}
	host := merged.Host
	if host == "" {
		host = "proxy"
	}
	env := &NetworkEnvelope{
		Host:      host,
		Target:    merged.Target,
		Direction: merged.Direction,
		Scope:     merged.Scope,
	}
	baseCfg := fault.FaultConfig{Duration: dur}
	return decodeNetworkFault(req.Type, env, cfg, baseCfg, resolve)
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

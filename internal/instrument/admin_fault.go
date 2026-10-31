package instrument

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

type faultSlot struct {
	decision        *evaluator.Decision
	req             *wire.FaultRequest
	lastConfirmedAt time.Time
}

type demoEvaluator struct {
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
func (d *demoEvaluator) Evaluate(_ context.Context, req evaluator.Request) *evaluator.Decision {
	// Admin slots arm the request path. The post-response point would fire
	// every armed inline fault a second time per request.
	if req.Point == evaluator.Transient {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	ids := d.sortedIDsLocked()
	for _, cat := range []string{"inline", "network", "resource"} {
		for _, id := range ids {
			slot := d.slots[id]
			if slot.req.EffectiveCategory() == cat && slot.decision != nil {
				return slot.decision
			}
		}
	}
	return nil
}

// sortedIDsLocked returns slot IDs in lexicographic order. Caller must hold d.mu.
func (d *demoEvaluator) sortedIDsLocked() []string {
	ids := make([]string, 0, len(d.slots))
	for id := range d.slots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Set installs or replaces the slot for req.ID.
// If req.ID is empty, it falls back to effectiveCategory.
func (d *demoEvaluator) Set(decision *evaluator.Decision, req *wire.FaultRequest) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.slots == nil {
		d.slots = make(map[string]*faultSlot)
	}

	id := req.ID
	if id == "" {
		id = req.EffectiveCategory()
	}

	d.slots[id] = &faultSlot{
		decision:        decision,
		req:             req,
		lastConfirmedAt: time.Now(),
	}
}

// ClearSlot deletes the fault slot with the given ID. ID matches the key used
// by Set (req.ID, or effectiveCategory when req.ID is empty).
func (d *demoEvaluator) ClearSlot(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.slots, id)
}

// Clear deactivates all faults.
func (d *demoEvaluator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.slots = make(map[string]*faultSlot)
}

// Confirm bumps lastConfirmedAt to now for the given ID.
func (d *demoEvaluator) Confirm(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s, ok := d.slots[id]; ok {
		s.lastConfirmedAt = time.Now()
	}
}

// ActiveIDs returns the IDs of all currently-armed slots, in lexicographic order.
func (d *demoEvaluator) ActiveIDs() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sortedIDsLocked()
}

// StaleSlots returns IDs whose lastConfirmedAt is older than maxAge.
func (d *demoEvaluator) StaleSlots(maxAge time.Duration) []string {
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
func (d *demoEvaluator) Active() []*wire.FaultRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ids := d.sortedIDsLocked()
	out := make([]*wire.FaultRequest, 0, len(ids))

	for _, cat := range []string{"inline", "network", "resource"} {
		for _, id := range ids {
			slot := d.slots[id]
			if slot.req != nil && slot.req.EffectiveCategory() == cat {
				out = append(out, slot.req)
			}
		}
	}
	return out
}

// faultAdminHandler serves the /admin/fault endpoint against the demo
// evaluator Serve composed AFTER the host rule evaluator: arming a slot here
// can never replace or tear down the host's evaluator, cache-box, or the
// process-lifetime fault registry — the old lazily-configuring zero-arg
// handler that could is gone.
//
// Interaction with manteion: admin POSTs key the slot by req.ID (or the
// effectiveCategory when ID is empty). When connected, every 200 poll answer
// runs reconciliation in apply, which drops slot IDs not present in the
// server's active_faults response (a 304 applies nothing). And because admin
// POST never calls Confirm, the fault watchdog will reap admin slots after the
// grace period (watchdogGrace: three poll cadences, at least 30s; 90s with the
// defaults). Treat admin faults as short-lived overrides when connected to
// manteion; in offline mode they persist until DELETEd.
func faultAdminHandler(eval *demoEvaluator, reg *interceptor.FaultRegistry, arm armFunc, opts ...decodeOption) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// wire.Route manually to support DELETE /admin/fault/{category}
		path := r.URL.Path

		switch r.Method {
		case http.MethodPost:
			handleFaultPost(w, r, eval, arm, opts)
		case http.MethodDelete:
			// check if path is /admin/fault or /admin/fault/{category}
			if path == "/admin/fault" || path == "/admin/fault/" {
				ids := eval.ActiveIDs()
				eval.Clear()
				stopFaults(reg, ids...)
			} else {
				// strip /admin/fault/ to get ID
				id := path[len("/admin/fault/"):]
				eval.ClearSlot(id)
				stopFaults(reg, id)
			}
			json.NewEncoder(w).Encode(wire.FaultStatus{Active: false})
		case http.MethodGet:
			reqs := eval.Active()
			if len(reqs) > 0 {
				json.NewEncoder(w).Encode(wire.FaultStatus{Active: true, Faults: reqs})
			} else {
				json.NewEncoder(w).Encode(wire.FaultStatus{Active: false})
			}
		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func handleFaultPost(w http.ResponseWriter, r *http.Request, eval *demoEvaluator, arm armFunc, opts []decodeOption) {
	var req wire.FaultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}

	// Name = slot id, matching Set's key, so DELETE can stop the running
	// fault through the registry under the same name (see applyActiveFault).
	id := req.ID
	if id == "" {
		id = req.EffectiveCategory()
	}

	f, err := buildFault(id, req, opts...)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	mode := evaluator.Inline
	if req.EffectiveCategory() != "inline" {
		mode = evaluator.Background
	}

	decision := &evaluator.Decision{
		Name:   id,
		Fault:  f,
		Reason: "admin",
		Mode:   mode,
	}
	eval.Set(decision, &req)
	armDecision(arm, evaluator.Egress, decision)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(wire.FaultStatus{Active: true, Faults: []*wire.FaultRequest{&req}})
}

// buildFault constructs a Fault from a wire.FaultRequest via the single decode
// path shared with compiled rules. Used by admin.go (handleFaultPost) and
// register.go (applyActiveFault) — the admin endpoint therefore gets the
// same envelope validation and ramp support as rule-attached faults.
func buildFault(id string, req wire.FaultRequest, opts ...decodeOption) (fault.Fault, error) {
	return decodeFault(id, &req, buildDecodeConfig(opts))
}

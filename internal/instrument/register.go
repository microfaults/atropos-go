package instrument

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// registerTimeout is the default per-call deadline for Register.
const registerTimeout = 5 * time.Second

// registerWith POSTs a registration to manteion.
// It marshals req, POSTs to baseURL+"/api/v1/sdk/register" with a 5s timeout
// (registerTimeout) layered onto ctx, and decodes the response. Response body
// is read under a 1 MiB cap; a truncated/erroring read still surfaces as a
// decode failure below, so dropping the ReadAll error loses no diagnostic value.
func registerWith(ctx context.Context, hc *http.Client, baseURL string, req wire.RegisterRequest) (wire.RegisterResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return wire.RegisterResponse{}, fmt.Errorf("marshal register request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/v1/sdk/register", bytes.NewReader(body))
	if err != nil {
		return wire.RegisterResponse{}, fmt.Errorf("new register request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := hc.Do(httpReq)
	if err != nil {
		return wire.RegisterResponse{}, fmt.Errorf("send register request: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
	if httpResp.StatusCode != http.StatusCreated {
		return wire.RegisterResponse{}, fmt.Errorf("register returned status %d: %s",
			httpResp.StatusCode, string(respBody))
	}

	var resp wire.RegisterResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return wire.RegisterResponse{}, fmt.Errorf("decode register response: %w", err)
	}
	return resp, nil
}

// applyTargets names the SDK objects apply mutates. A nil target means "this
// SDK instance doesn't support that capability"; a document that needs a
// missing one is a hard error (see apply), except the replay-set puller,
// whose absence is reported per document.
type applyTargets struct {
	// Evaluator receives the decoded rule set. Required if the rules are non-empty.
	Evaluator *evaluator.StaticEvaluator
	// DemoEval receives the active faults. Required if active_faults is non-empty.
	DemoEval *demoEvaluator
	// CacheBox receives the freeze config. Required if freeze_cfg is present.
	CacheBox *cachebox.CacheBox
	// ReplaySet converges the cache box's installed replay set on the one the
	// document names. Nil means no replay-set support: a document naming a
	// replay set is reported, and an installed set is left alone.
	ReplaySet *replaySetPuller
	// NetworkResolver maps logical targets to listen/upstream pairs for network
	// fault proxies. Without one, rules and active faults with network
	// host=proxy are skipped per rule and counted.
	NetworkResolver wire.NetworkResolver
	// CacheDrain, if set, observes every authoritative rule set apply
	// installs -- polled, registered or pushed, and the empty set included
	// (it is the common drain trigger). It diffs against the
	// previously-seen active recording phases, under ApplyMu, and a flush +
	// W3 drain report goes out for any that just ended (design doc Q2) once
	// ApplyMu is released. Optional — nil means this SDK doesn't need
	// automatic drain-report generation (e.g. it never records, or triggers
	// draining some other way).
	CacheDrain *cacheDrainTracker
	// Registry holds every running background fault; rule/slot removal stops
	// faults through it (nil-safe: a nil registry stops nothing).
	Registry *interceptor.FaultRegistry
	// DecodeOpts are host-supplied decode hooks (e.g. the inline toxic
	// host), appended to the resolver option on every decode.
	DecodeOpts []decodeOption
	// Arm, when set, starts a decoded background fault as soon as its rule
	// or slot applies (eager arming, design §3.D1). Nil keeps lazy arming.
	Arm armFunc
	// ApplyMu serializes apply calls against these targets. Core.targets
	// hands every applyTargets it builds the one mutex the core owns, so the
	// manteion client's applies and the /admin/rules push never interleave.
	// Nil (targets built by hand) means no locking.
	ApplyMu *sync.Mutex
}

// decodeOpts is the option set every decode under these targets uses.
func (t applyTargets) decodeOpts() []decodeOption {
	opts := append([]decodeOption(nil), t.DecodeOpts...)
	if t.NetworkResolver != nil {
		opts = append(opts, withNetworkResolver(t.NetworkResolver))
	}
	return opts
}

// applyScope selects the categories one apply call reconciles; the others
// stay exactly as they are.
type applyScope uint8

const (
	scopeRules applyScope = 1 << iota
	scopeFaults
	scopeFreeze
	scopeReplaySet
	scopeAll = scopeRules | scopeFaults | scopeFreeze | scopeReplaySet
)

// applyReport is what one apply could not hold, in the shape the status
// report carries (spec §2): Unsupported lists the rules and active-fault
// slots this host cannot execute; Errors the rules that did not decode, the
// slots that did not build, a freeze config that did not validate and a
// replay set no puller here can fetch. Both lists are non-nil (empty, not
// null): an empty list clears manteion's previous report.
type applyReport struct {
	Unsupported []wire.UnsupportedRule
	Errors      []string
	// Confirmed lists, in document order, the document's active-fault slots
	// this apply holds and confirmed: afterwards they are exactly the slots
	// held, save an /admin/fault slot armed meanwhile. Nil when apply left
	// the slots alone (no active_faults, or a scope without scopeFaults). Not
	// part of the status report: the manteion client keeps it to re-confirm
	// the slots on every later answer (confirmSlots).
	Confirmed []string
}

// apply reconciles targets with one desired-state document. It is the only
// place rules, active faults, the freeze config and the replay set are
// applied: the manteion client calls it for the register answer and every
// 200 poll, the /admin/rules push for every pushed rule set, so decode,
// atomic replace, stop-by-name, eager arming and drain observation happen
// once per delivery whichever way it arrives (design review §5;
// desired-state protocol §3). Calls are serialized on targets.ApplyMu.
//
// No network I/O runs under that lock: reconcile applies the document under
// it and collects the recording phases the rule set ended, and apply sends
// their drain reports (CacheDrain.Drain) after releasing it, before it
// returns. A caller holding a lock of its own across the apply (the manteion
// client's applyMu) calls reconcile and drains once it has released that
// lock too.
//
// scope selects the categories apply touches; the rest stay exactly as they
// are. The client applies scopeAll. The /admin/rules push applies scopeRules,
// so a pushed rule set never clears an admin fault slot, the delay source or
// the installed replay set.
//
// Rules contract: a nil slice means 'no change' (absent field -- a legacy
// manteion, or a payload that doesn't carry rules); a non-nil slice is
// authoritative desired state INCLUDING when empty. Empty-clears is what
// makes the poll loop a true reconciler: phase teardown clears rules via
// a push fanout that can miss instances (deregistered, filtered, down),
// and an instance that missed it must converge to zero rules on its next
// poll -- otherwise it keeps replaying/faulting forever. Active faults
// follow the same contract: nil is no change, [] clears every slot.
//
// Freeze config and replay set: absent means none desired. A document
// without freeze_cfg clears the delay source; one without replay_set clears
// the installed set (through the puller). freeze_cfg.context scopes the
// delay source to its (experiment_id, phase_id); the replay set is installed
// under the pair replayScope picks.
//
// No document: a body with neither an etag nor rules is manteion's degraded
// register answer -- it could not materialize the desired state. Under
// scopeAll apply applies nothing at all and returns an empty report: the
// instance keeps its rules, slots, delay source and replay set.
//
// An item this host cannot hold never fails the document: it is reported
// and the rest still applies. Rules and slots it cannot execute go to
// Unsupported (and are counted in atropos_rules_unsupported_total); a rule
// that does not decode ("rule <name>: decode: ..."), a slot that does not
// build ("active_fault <id>: ..."), a freeze config that does not validate
// ("freeze_cfg: ...") and a replay set with no puller to fetch it
// ("replay_set <id>: ...") go to Errors. A slot that cannot be held installs
// nothing and clears an earlier slot under the same id, stopping its running
// fault, so the slots held are exactly the ones report.Confirmed names. A
// rejected freeze config installs nothing; an earlier delay source stays.
//
// Hard errors -- a category present with no target to apply it to: rules
// without an Evaluator, active faults without a DemoEval, a freeze config
// without a CacheBox -- are returned before anything is applied, so the
// targets keep what they held. They are prefixed by category ("apply rules:
// ...", "apply active_faults: ...", "apply freeze_cfg: ...") so log grepping
// can filter a single bootstrap phase without ambiguity.
func apply(sync wire.RuleSync, targets applyTargets, scope applyScope) (applyReport, error) {
	report, ended, err := reconcile(sync, targets, scope)
	targets.CacheDrain.Drain(ended) // network I/O: after targets.ApplyMu is released
	return report, err
}

// reconcile is apply but for the drain reports: it applies the document
// under targets.ApplyMu and returns, with the report, the recording phases
// the rule set ended, whose drain reports the caller sends once it holds no
// lock (CacheDrain.Drain). A hard error applies nothing and ends no phase.
func reconcile(sync wire.RuleSync, targets applyTargets, scope applyScope) (report applyReport, ended []recordingPhaseKey, err error) {
	if targets.ApplyMu != nil {
		targets.ApplyMu.Lock()
		defer targets.ApplyMu.Unlock()
	}
	report = applyReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{}}

	if scope == scopeAll && sync.Etag == "" && sync.Rules == nil {
		slog.Default().Warn("desired state: manteion sent no document; keeping the current state")
		return report, nil, nil
	}

	if scope&scopeRules != 0 && len(sync.Rules) > 0 && targets.Evaluator == nil {
		return report, nil, fmt.Errorf("apply rules: no Evaluator target for %d rules", len(sync.Rules))
	}
	if scope&scopeFaults != 0 && len(sync.ActiveFaults) > 0 && targets.DemoEval == nil {
		return report, nil, errors.New("apply active_faults: no DemoEval target")
	}
	if scope&scopeFreeze != 0 && sync.FreezeCfg != nil && targets.CacheBox == nil {
		return report, nil, errors.New("apply freeze_cfg: no CacheBox target")
	}

	if scope&scopeRules != 0 && sync.Rules != nil {
		if targets.Evaluator != nil {
			out := decodeRuleSet(sync.Rules, targets.decodeOpts()...)
			applyRuleSet(targets.Registry, targets.Evaluator, out.Rules, targets.Arm)
			report.Unsupported = append(report.Unsupported, out.Unsupported...)
			report.Errors = append(report.Errors, out.Errors...)
		}
		// Observe every authoritative rule set, INCLUDING empty: the recording
		// phase whose rule drops out most often leaves the set empty, and that
		// transition is exactly what triggers the flush + drain report.
		ended = targets.CacheDrain.Observe(sync.Rules)
	}

	if scope&scopeFaults != 0 && sync.ActiveFaults != nil && targets.DemoEval != nil {
		// Manteion is authoritative: reconciliation drops any locally-armed slot
		// not present in active_faults. This includes admin-set slots, which is
		// intentional — see the doc on faultAdminHandler.
		activeIDs := make(map[string]bool)
		for _, req := range sync.ActiveFaults {
			id := req.ID
			if id == "" {
				id = req.EffectiveCategory()
			}
			activeIDs[id] = true
		}

		for _, id := range targets.DemoEval.ActiveIDs() {
			if !activeIDs[id] {
				targets.DemoEval.ClearSlot(id)
				// Manteion dropped the slot; a running background fault it
				// started must not keep executing under a withdrawn intent.
				stopFaults(targets.Registry, id)
			}
		}

		// Apply / refresh slots that ARE in the document. Confirm refreshes the
		// watchdog heartbeat so the slot survives until its server-side lifetime
		// ends. A slot that cannot be held is reported, and an earlier slot
		// under its id is cleared and its running fault stopped: the held
		// slots must be exactly the confirmed ones, or the client would keep
		// an unbuildable redefinition's predecessor alive on every 304.
		report.Confirmed = make([]string, 0, len(sync.ActiveFaults))
		for _, req := range sync.ActiveFaults {
			id := req.ID
			if id == "" {
				id = req.EffectiveCategory()
			}
			if err := applyActiveFault(req, targets); err != nil {
				var unsup *UnsupportedFaultError
				if errors.As(err, &unsup) {
					report.Unsupported = append(report.Unsupported, wire.UnsupportedRule{Rule: id, Category: unsup.Category})
				} else {
					report.Errors = append(report.Errors, fmt.Sprintf("active_fault %s: %v", id, err))
				}
				targets.DemoEval.ClearSlot(id)
				stopFaults(targets.Registry, id)
				report.Confirmed = slices.DeleteFunc(report.Confirmed, func(c string) bool { return c == id })
				continue
			}
			targets.DemoEval.Confirm(id)
			if !slices.Contains(report.Confirmed, id) { // an id repeated in the document is held once
				report.Confirmed = append(report.Confirmed, id)
			}
		}
	}

	if scope&scopeFreeze != 0 {
		switch {
		case sync.FreezeCfg != nil:
			if err := applyFreezeCfg(*sync.FreezeCfg, targets.CacheBox); err != nil {
				report.Errors = append(report.Errors, "freeze_cfg: "+err.Error())
			}
		case targets.CacheBox != nil:
			targets.CacheBox.ClearDelaySource() // none desired
		}
	}

	if scope&scopeReplaySet != 0 {
		if sync.ReplaySet != nil && targets.ReplaySet == nil {
			report.Errors = append(report.Errors, fmt.Sprintf("replay_set %s: this host has no replay-set puller", sync.ReplaySet.ID))
		} else {
			targets.ReplaySet.Reconcile(sync.ReplaySet, replayScope(sync))
		}
	}

	return report, ended, nil
}

// replayScope is the (experiment_id, phase_id) pair the document's replay
// set is installed under. The cache-box serves an installed set only to a
// replay rule whose context names that pair (replayLookup in
// internal/interceptor/cachebox.go), so the pair is the set's address. In
// order: the ref's own pair (spec §2: replay_set names the pair it is
// installed under), then the context of the first replay or
// replay_with_delay rule, then freeze_cfg's context. Each counts only with
// both ids set. With none the zero pair comes back and the puller reports
// the set failed instead of guessing.
func replayScope(sync wire.RuleSync) cachebox.PhasePair {
	if ref := sync.ReplaySet; ref != nil && ref.ExperimentID != "" && ref.PhaseID != "" {
		return cachebox.PhasePair{ExperimentID: ref.ExperimentID, PhaseID: ref.PhaseID}
	}
	for _, r := range sync.Rules {
		if cb := r.CacheBox; cb != nil && (cb.Mode == "replay" || cb.Mode == "replay_with_delay") {
			if c := cb.Context; c != nil && c.ExperimentID != "" && c.PhaseID != "" {
				return cachebox.PhasePair{ExperimentID: c.ExperimentID, PhaseID: c.PhaseID}
			}
		}
	}
	if fc := sync.FreezeCfg; fc != nil {
		if c := fc.Context; c != nil && c.ExperimentID != "" && c.PhaseID != "" {
			return cachebox.PhasePair{ExperimentID: c.ExperimentID, PhaseID: c.PhaseID}
		}
	}
	return cachebox.PhasePair{}
}

// applyActiveFault builds a Fault from a wire.FaultRequest and installs it on the
// demoEvaluator. Uses the shared buildFault dispatcher. A fault this host
// cannot execute is counted and logged here and returned as the
// *UnsupportedFaultError, for apply to report; nothing is installed for it,
// nor for a fault that does not build.
//
// Decision.Name is the slot id (the same key Set stores under): it becomes
// the fault registry's key, so ClearSlot(id) can stop the running fault by
// the identical name. A shared constant name here would collapse distinct
// slots into one registry entry -- deduplicating faults that should
// coexist and stopping strangers on clear.
func applyActiveFault(req wire.FaultRequest, targets applyTargets) error {
	id := req.ID
	if id == "" {
		id = req.EffectiveCategory()
	}
	f, err := buildFault(id, req, targets.decodeOpts()...)
	if err != nil {
		var unsup *UnsupportedFaultError
		if errors.As(err, &unsup) {
			metrics.RulesUnsupportedTotal.WithLabelValues(unsup.Category).Inc()
			slog.Default().Warn("active fault skipped: not executable by this host",
				"slot", id, "category", unsup.Category, "fault_type", unsup.FaultType)
		}
		return err
	}

	mode := evaluator.Inline
	if req.EffectiveCategory() != "inline" {
		mode = evaluator.Background
	}

	d := &evaluator.Decision{
		Name:   id,
		Fault:  f,
		Reason: "register",
		Mode:   mode,
	}
	targets.DemoEval.Set(d, &req)
	armDecision(targets.Arm, evaluator.Egress, d)
	return nil
}

// applyFreezeCfg validates a wire.DelayRequest and installs its distribution
// delay source on the CacheBox: scoped to the context's (experiment_id,
// phase_id) when the request carries a context, unscoped otherwise. A
// context naming only one of the pair is rejected rather than installed
// unscoped, which would delay every replay. A rejected request installs
// nothing. Shared by apply (freeze_cfg) and POST /admin/cachebox/delay.
func applyFreezeCfg(req wire.DelayRequest, cb *cachebox.CacheBox) error {
	if req.Mu < 0 {
		return fmt.Errorf("mu must be >= 0, got %f", req.Mu)
	}
	if req.Sigma < 0 {
		return fmt.Errorf("sigma must be >= 0, got %f", req.Sigma)
	}
	if c := req.Context; c != nil && (c.ExperimentID == "") != (c.PhaseID == "") {
		return errors.New("context needs both experiment_id and phase_id")
	}
	src := cachebox.NewDistributionDelaySource(req.Mu, req.Sigma, req.Seed)
	if c := req.Context; c != nil {
		cb.SetScopedDelaySource(src, cachebox.PhasePair{ExperimentID: c.ExperimentID, PhaseID: c.PhaseID})
		return nil
	}
	cb.SetDelaySource(src)
	return nil
}

// reapStaleSlots is one watchdog pass (the manteion client's watchSlots):
// every slot not confirmed within grace is cleared and its running faults
// stopped, and the ids cleared are returned. The pass holds applyMu, the
// core's apply mutex (nil = no locking), from the staleness check to the last
// clear, so a slot that an apply or the client's confirmSlots refreshes in
// between is never dropped.
func reapStaleSlots(eval *demoEvaluator, reg *interceptor.FaultRegistry, applyMu *sync.Mutex, grace time.Duration, logger interface{ Warn(string, ...any) }) []string {
	if applyMu != nil {
		applyMu.Lock()
		defer applyMu.Unlock()
	}
	stale := eval.StaleSlots(grace)
	for _, cat := range stale {
		eval.ClearSlot(cat)
		// Clearing the slot only stops NEW starts; the zombie guard
		// is only real if the running instance dies too.
		stopFaults(reg, cat)
		if logger != nil {
			logger.Warn("watchdog: dropped stale fault slot", "category", cat, "grace", grace)
		}
	}
	return stale
}

// stopFaults cancels the running background faults started under keys.
// Rule removal is the platform's fault-stop signal, so every path that
// drops a rule or slot funnels here. Nil-safe on the registry.
func stopFaults(reg *interceptor.FaultRegistry, keys ...string) {
	for _, k := range keys {
		reg.Stop(k)
	}
}

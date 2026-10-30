package instrument

import (
	"encoding/json"
	"fmt"
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// applyRuleSet atomically replaces eval's rules and stops any running
// background fault whose owning rule dropped out of the set. This is the
// single rule-application path (poll/register Apply and the push endpoint
// both funnel here): a background fault must not outlive the rule that
// started it, and the rule's removal -- however it arrives -- is the stop
// signal. Replacement happens before the stops so a re-evaluated request
// cannot restart a fault under the outgoing rule set.
func applyRuleSet(reg *interceptor.FaultRegistry, eval *evaluator.StaticEvaluator, rules []evaluator.StaticRule, arm armFunc) {
	prev := eval.Rules()
	eval.SetRules(rules)

	next := make(map[string]bool, len(rules))
	for _, r := range rules {
		next[r.Name] = true
	}
	for _, r := range prev {
		if !next[r.Name] {
			stopFaults(reg, r.Name)
		}
	}
	// Eager arming (hosts that opt in): a background fault takes effect
	// when its rule applies, not when the next request happens to match.
	for i := range rules {
		d := rules[i].Decision
		armDecision(arm, rules[i].Point, &d)
	}
}

// armFunc starts a decoded background fault at apply time. Only hosts that
// own long-lived connection policy (the sidecar) supply one; the in-process
// host keeps lazy arming on the first matching request.
type armFunc func(point evaluator.InjectionPoint, d *evaluator.Decision)

// armDecision applies arm to d when the host arms eagerly and the fault is
// a background one. Inline faults are per-request by nature and stay lazy,
// as do Transient rules.
func armDecision(arm armFunc, point evaluator.InjectionPoint, d *evaluator.Decision) {
	// Transient rules are request-triggered by definition: their status
	// label only exists once a response has completed.
	if arm == nil || d == nil || d.Fault == nil || point == evaluator.Transient || interceptor.IsInlineFault(d.Fault) {
		return
	}
	arm(point, d)
}

// rulesAdminHandler serves runtime rule management on the host evaluator
// (mounted at /admin/rules). It is the push side of the single rule-apply
// path: POST goes through applyRules exactly as a polled set does, so
// decode, atomic replace, stop-by-name, eager arming and drain observation
// happen once per delivery. The push body carries no version, so the
// poll's ruleVersion (manteion_client.go, fetchRules) is left untouched.
//
// Supported methods:
//   - GET:  200 + JSON-encoded current rule list (empty array if nil)
//   - POST: body is []wire.CompiledRule; null clears the set (teardown);
//     204 on success, 400 on bad JSON or a rule set that does not apply
//   - Other: 405
//
// targets.Evaluator is required.
func rulesAdminHandler(targets applyTargets) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			rules := targets.Evaluator.Rules()
			if rules == nil {
				rules = []evaluator.StaticRule{}
			}
			json.NewEncoder(w).Encode(rules)

		case http.MethodPost:
			var compiled []wire.CompiledRule
			if err := json.NewDecoder(r.Body).Decode(&compiled); err != nil {
				jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
				return
			}
			if compiled == nil {
				compiled = []wire.CompiledRule{} // null = authoritative clear
			}
			if err := applyRules(compiled, targets); err != nil {
				jsonError(w, fmt.Sprintf("apply rules: %s", err), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

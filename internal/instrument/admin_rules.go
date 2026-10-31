package instrument

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// applyRuleSet atomically replaces eval's rules and stops any running
// background fault whose owning rule dropped out of the set. This is the
// single rule-application path (apply's rules branch, which poll, register
// and the push endpoint all go through): a background fault must not
// outlive the rule that started it, and the rule's removal -- however it
// arrives -- is the stop signal. Replacement happens before the stops so a
// re-evaluated request cannot restart a fault under the outgoing rule set.
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
// (mounted at /admin/rules). It is the push side of the single apply path:
// POST goes through apply exactly as a polled document does, scoped to its
// rules (scopeRules), so decode, atomic replace, stop-by-name, eager arming
// and drain observation happen once per delivery while admin fault slots,
// the delay source and the installed replay set stay untouched. The push
// body carries no version, so the poll's ruleVersion (manteion_client.go,
// fetchRules) is left untouched.
//
// Connected (client non-nil), a push is a short-lived override, like an
// /admin/fault slot: once it applied (204 or 422) the client forgets the
// ETag it holds (pushOverride), so the next poll is unconditional and its 200
// re-applies manteion's desired state over the pushed set -- which lasts
// until that poll completes, at most one long-poll hold plus the apply. The
// push sends no status report. Offline (nil client), a pushed set holds until
// the next push.
//
// Supported methods:
//   - GET:  200 + JSON-encoded current rule list (empty array if nil)
//   - POST: body is []wire.CompiledRule; null clears the set (teardown).
//     204 when every rule applied or was skipped as unsupported; 400 on bad
//     JSON; 422 when some rules did not decode -- the pushed set, less the
//     skipped rules, replaced the installed one (an empty set when none
//     decoded), and the body names the skipped ones; 500 when this host
//     cannot hold rules (nothing applied)
//   - Other: 405
//
// targets.Evaluator is required.
func rulesAdminHandler(targets applyTargets, client *ManteionClient) http.Handler {
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
			report, err := apply(wire.RuleSync{Rules: compiled}, targets, scopeRules)
			if err != nil {
				jsonError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			client.pushOverride()
			if len(report.Errors) > 0 {
				jsonError(w, strings.Join(report.Errors, "; "), http.StatusUnprocessableEntity)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

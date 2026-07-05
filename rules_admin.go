package atropos

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// applyRuleSet atomically replaces eval's rules and stops any running
// background fault whose owning rule dropped out of the set. This is the
// single rule-application path (poll/register Apply and the push endpoint
// both funnel here): a background fault must not outlive the rule that
// started it, and the rule's removal -- however it arrives -- is the stop
// signal. Replacement happens before the stops so a re-evaluated request
// cannot restart a fault under the outgoing rule set.
func applyRuleSet(eval *StaticEvaluator, rules []StaticRule) {
	prev := eval.Rules()
	eval.SetRules(rules)

	next := make(map[string]bool, len(rules))
	for _, r := range rules {
		next[r.Name] = true
	}
	for _, r := range prev {
		if !next[r.Name] {
			stopBackgroundFaults(r.Name)
		}
	}
}

// RulesAdminHandler returns an http.Handler for runtime rule management on a
// StaticEvaluator.
//
// Supported methods:
//   - GET:  200 + JSON-encoded current rule list (empty array if nil)
//   - POST: decode body as []CompiledRule (wire format), convert via
//     DecodeCompiledRules, atomically replace via SetRules, 204
//   - Other: 405
//
// opts are forwarded to DecodeCompiledRules (e.g. WithNetworkResolver).
func RulesAdminHandler(eval *StaticEvaluator, opts ...DecodeOption) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			rules := eval.Rules()
			if rules == nil {
				rules = []StaticRule{}
			}
			json.NewEncoder(w).Encode(rules)

		case http.MethodPost:
			var compiled []CompiledRule
			if err := json.NewDecoder(r.Body).Decode(&compiled); err != nil {
				jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
				return
			}
			if compiled == nil {
				applyRuleSet(eval, nil)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			rules, err := DecodeCompiledRules(compiled, opts...)
			if err != nil {
				jsonError(w, fmt.Sprintf("decode rules: %s", err), http.StatusBadRequest)
				return
			}
			applyRuleSet(eval, rules)
			w.WriteHeader(http.StatusNoContent)

		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

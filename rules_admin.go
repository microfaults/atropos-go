package atropos

import (
	"encoding/json"
	"fmt"
	"net/http"
)

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
				eval.SetRules(nil)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			rules, err := DecodeCompiledRules(compiled, opts...)
			if err != nil {
				jsonError(w, fmt.Sprintf("decode rules: %s", err), http.StatusBadRequest)
				return
			}
			eval.SetRules(rules)
			w.WriteHeader(http.StatusNoContent)

		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

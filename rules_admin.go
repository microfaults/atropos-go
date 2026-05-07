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
//   - POST: decode body as []StaticRule, atomically replace via SetRules, 204
//   - Other: 405
//
// Example:
//
//	mux.Handle("/admin/rules", atropos.RulesAdminHandler(eval))
//	// curl http://localhost:8080/admin/rules
//	// curl -X POST http://localhost:8080/admin/rules -d '[{"Name":"freeze-svc","Point":1}]'
func RulesAdminHandler(eval *StaticEvaluator) http.Handler {
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
			var rules []StaticRule
			if err := json.NewDecoder(r.Body).Decode(&rules); err != nil {
				jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
				return
			}
			eval.SetRules(rules)
			w.WriteHeader(http.StatusNoContent)

		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

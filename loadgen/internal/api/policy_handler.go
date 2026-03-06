package api

import (
	"net/http"

	"atropos-go/loadgen/internal/policy"
)

func (s *Server) handleCreatePolicy(w http.ResponseWriter, r *http.Request) {
	var rule policy.Rule
	if err := readJSON(r, &rule); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if rule.ID == "" {
		rule.ID = generateID()
	}

	if err := s.engine.AddRule(&rule); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, rule)
}

func (s *Server) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	rules := s.engine.ListRules()
	writeJSON(w, http.StatusOK, rules)
}

func (s *Server) handleDeletePolicy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.engine.RemoveRule(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

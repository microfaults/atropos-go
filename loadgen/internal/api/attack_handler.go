package api

import (
	"net/http"

	"github.com/microfaults/atropos-go/loadgen/internal/attacker"
)

func (s *Server) handleCreateAttack(w http.ResponseWriter, r *http.Request) {
	var cfg attacker.AttackConfig
	if err := readJSON(r, &cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if cfg.ID == "" {
		cfg.ID = generateID()
	}

	// If linked to a workload, propagate meta-trace-id.
	if cfg.WorkloadRef != "" {
		wl, ok := s.registry.Get(cfg.WorkloadRef)
		if !ok {
			writeError(w, http.StatusNotFound, "workload ref not found: "+cfg.WorkloadRef)
			return
		}
		if cfg.MetaTraceID == "" {
			cfg.MetaTraceID = wl.MetaTraceID
		}
	}

	attack, err := s.manager.Launch(r.Context(), cfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, attack)
}

func (s *Server) handleGetAttack(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	attack, ok := s.manager.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "attack not found: "+id)
		return
	}
	writeJSON(w, http.StatusOK, attack)
}

func (s *Server) handleStopAttack(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.manager.Stop(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

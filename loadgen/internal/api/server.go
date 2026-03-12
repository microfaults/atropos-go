package api

import (
	"encoding/json"
	"io"
	"net/http"

	"atropos-go/loadgen/internal/attacker"
	"atropos-go/loadgen/internal/policy"
	"atropos-go/loadgen/internal/workload"
)

const maxBodySize = 1 << 20 // 1 MiB

// Server holds shared dependencies and configures routing.
type Server struct {
	registry *workload.Registry
	manager  *attacker.Manager
	engine   *policy.Engine
	mux      *http.ServeMux
}

// NewServer creates an API server wired to the given components.
func NewServer(reg *workload.Registry, mgr *attacker.Manager, eng *policy.Engine) *Server {
	s := &Server{
		registry: reg,
		manager:  mgr,
		engine:   eng,
		mux:      http.NewServeMux(),
	}
	s.routes()
	return s
}

// Handler returns the underlying http.Handler for use with http.Server.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	// Workloads
	s.mux.HandleFunc("POST /api/v1/workloads", s.handleCreateWorkload)
	s.mux.HandleFunc("GET /api/v1/workloads", s.handleListWorkloads)
	s.mux.HandleFunc("DELETE /api/v1/workloads/{id}", s.handleDeleteWorkload)

	// Attacks
	s.mux.HandleFunc("POST /api/v1/attacks", s.handleCreateAttack)
	s.mux.HandleFunc("GET /api/v1/attacks/{id}", s.handleGetAttack)
	s.mux.HandleFunc("DELETE /api/v1/attacks/{id}", s.handleStopAttack)

	// Policies
	s.mux.HandleFunc("POST /api/v1/policies", s.handleCreatePolicy)
	s.mux.HandleFunc("GET /api/v1/policies", s.handleListPolicies)
	s.mux.HandleFunc("DELETE /api/v1/policies/{id}", s.handleDeletePolicy)

	// Status
	s.mux.HandleFunc("GET /api/v1/status", s.handleStatus)
}

// --- JSON helpers ---

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func readJSON(r *http.Request, v any) error {
	body := io.LimitReader(r.Body, maxBodySize)
	return json.NewDecoder(body).Decode(v)
}

// --- Status handler ---

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]any{
		"status":    "ok",
		"workloads": len(s.registry.List()),
		"attacks":   len(s.manager.List()),
		"policies":  len(s.engine.ListRules()),
	}
	writeJSON(w, http.StatusOK, status)
}

package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"atropos-go/loadgen/internal/workload"
)

func (s *Server) handleCreateWorkload(w http.ResponseWriter, r *http.Request) {
	var wl workload.Workload
	if err := readJSON(r, &wl); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if wl.ID == "" {
		wl.ID = generateID()
	}
	if wl.Status == "" {
		wl.Status = "running"
	}
	wl.RegisteredAt = time.Now()

	if err := s.registry.Register(&wl); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, wl)
}

func (s *Server) handleListWorkloads(w http.ResponseWriter, r *http.Request) {
	list := s.registry.List()
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleDeleteWorkload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.registry.Deregister(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

package atropos

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// CacheBoxPreloadHandler returns an http.Handler implementing the ATRO-6
// staged preload protocol (wire spec §W4): begin/chunk/commit/abort. A
// half-delivered preload is never visible to replay -- Commit is the only
// step that installs anything, and only on an exact count+checksum match
// (wire spec §W5); on mismatch staging is dropped and any previously
// installed set is left untouched.
//
// Routes (matched on method + last path segment), all POST:
//   - .../begin  → 200 {"ok":true} | 413 {"error":"too_large"} | 409 (unsupported key_strategy)
//   - .../chunk  → 200 {"staged_total":n} | 413 | 409 (no matching begin)
//   - .../commit → 200 {"ok":true,"loaded":N,"checksum":"..."} on match | 409 {"ok":false,...} on mismatch
//   - .../abort  → 200
//
// Example:
//
//	mux.Handle("/cachebox/preload/", atropos.CacheBoxPreloadHandler(cb))
func CacheBoxPreloadHandler(cb *CacheBox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")

		suffix := ""
		if i := strings.LastIndex(r.URL.Path, "/"); i >= 0 {
			suffix = r.URL.Path[i+1:]
		}

		switch suffix {
		case "begin":
			handlePreloadBegin(w, r, cb)
		case "chunk":
			handlePreloadChunk(w, r, cb)
		case "commit":
			handlePreloadCommit(w, r, cb)
		case "abort":
			handlePreloadAbort(w, r, cb)
		default:
			jsonError(w, "not found", http.StatusNotFound)
		}
	})
}

func handlePreloadBegin(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var req PreloadBeginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}
	result := cb.PreloadBegin(req.ExperimentID, req.PhaseID, req.KeyStrategy, req.MaxBytes)
	if result.UnsupportedStrategy {
		jsonError(w, fmt.Sprintf("unsupported key_strategy %q", req.KeyStrategy), http.StatusConflict)
		return
	}
	_ = json.NewEncoder(w).Encode(PreloadBeginResponse{OK: true})
}

func handlePreloadChunk(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var req PreloadChunkRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}
	entries := make([]*cachebox.Entry, len(req.Entries))
	for i := range req.Entries {
		entries[i] = cachebox.WireToEntry(&req.Entries[i])
	}
	result := cb.PreloadChunk(req.ExperimentID, req.PhaseID, req.ChunkSeq, entries)
	switch {
	case result.NoBegin:
		jsonError(w, "no active preload for this pair -- call begin first", http.StatusConflict)
	case result.TooLarge:
		jsonError(w, "too_large", http.StatusRequestEntityTooLarge)
	default:
		_ = json.NewEncoder(w).Encode(PreloadChunkResponse{StagedTotal: result.StagedTotal})
	}
}

func handlePreloadCommit(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var req PreloadCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}
	result := cb.PreloadCommit(req.ExperimentID, req.PhaseID, req.TotalEntries, req.Checksum)
	if result.NoBegin {
		jsonError(w, "no active preload for this pair -- call begin first", http.StatusConflict)
		return
	}
	if !result.OK {
		w.WriteHeader(http.StatusConflict)
	}
	_ = json.NewEncoder(w).Encode(PreloadCommitResponse{
		OK: result.OK, Loaded: result.Loaded, Checksum: result.Checksum,
	})
}

func handlePreloadAbort(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var req PreloadAbortRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}
	cb.PreloadAbort(req.ExperimentID, req.PhaseID)
	w.WriteHeader(http.StatusOK)
}

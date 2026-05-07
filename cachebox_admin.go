package atropos

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// CacheBoxAdminHandler returns an http.Handler for runtime cache-box control.
//
// Supported routes (matched on method + last path segment):
//   - GET  /admin/cachebox          → 200 with JSON stats snapshot
//   - POST /admin/cachebox/delay    → 204; body: {mu, sigma, seed}
//   - DELETE /admin/cachebox        → 204; clears the store
//
// Example:
//
//	mux.Handle("/admin/cachebox/", atropos.CacheBoxAdminHandler(cb))
//	// curl http://localhost:8080/admin/cachebox
//	// curl -X POST http://localhost:8080/admin/cachebox/delay -d '{"mu":1.0,"sigma":0.5,"seed":42}'
//	// curl -X DELETE http://localhost:8080/admin/cachebox
func CacheBoxAdminHandler(cb *CacheBox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		suffix := ""
		if i := strings.LastIndex(r.URL.Path, "/"); i >= 0 && i < len(r.URL.Path)-1 {
			suffix = r.URL.Path[i+1:]
		}

		switch {
		case r.Method == http.MethodGet && suffix != "delay":
			handleCacheBoxStats(w, cb)
		case r.Method == http.MethodPost && suffix == "delay":
			handleCacheBoxDelay(w, r, cb)
		case r.Method == http.MethodPost && suffix == "entries":
			handleCacheBoxPreload(w, r, cb)
		case r.Method == http.MethodDelete:
			cb.Store().Clear()
			w.WriteHeader(http.StatusNoContent)
		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func handleCacheBoxStats(w http.ResponseWriter, cb *CacheBox) {
	json.NewEncoder(w).Encode(cb.Stats())
}

// DelayRequest is the JSON body for POST /admin/cachebox/delay.
type DelayRequest struct {
	Mu    float64 `json:"mu"`
	Sigma float64 `json:"sigma"`
	Seed  uint64  `json:"seed"`
}

func handleCacheBoxPreload(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var entries []cachebox.WireEntry
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&entries); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}
	store := cb.Store()
	for i := range entries {
		e := cachebox.WireToEntry(&entries[i])
		store.Put(e.Key, e)
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleCacheBoxDelay(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var req DelayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
		return
	}
	if req.Mu < 0 {
		jsonError(w, "mu must be >= 0", http.StatusBadRequest)
		return
	}
	if req.Sigma < 0 {
		jsonError(w, "sigma must be >= 0", http.StatusBadRequest)
		return
	}
	cb.SetDelaySource(cachebox.NewDistributionDelaySource(req.Mu, req.Sigma, req.Seed))
	w.WriteHeader(http.StatusNoContent)
}

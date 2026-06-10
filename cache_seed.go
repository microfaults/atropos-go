package atropos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

const seedTimeout = 30 * time.Second

type seedResponse struct {
	Entries []cachebox.WireEntry `json:"entries"`
}

// Seed fetches cached entries from manteion and inserts them into the store.
// Call during service startup, after Register but before serving traffic,
// to warm the cache for replay experiments.
//
// phaseID identifies the experiment phase whose recorded entries to load —
// manteion keys its cache store by (phase, service).
//
// Returns the number of entries seeded. Fit parameters (mu/sigma for
// replay_with_delay) come through RegisterResponse.FreezeCfg via Apply,
// not through Seed.
func Seed(ctx context.Context, baseURL, service, phaseID string, store CacheBoxStore) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, seedTimeout)
	defer cancel()

	q := url.Values{"service": {service}, "phase_id": {phaseID}}
	fullURL := baseURL + "/api/v1/cache/entries?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return 0, fmt.Errorf("seed: new request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("seed: request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20)) // 64 MiB cap
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("seed: server returned %d: %s", resp.StatusCode, string(body))
	}

	var envelope seedResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, fmt.Errorf("seed: decode response: %w", err)
	}

	for i := range envelope.Entries {
		entry := cachebox.WireToEntry(&envelope.Entries[i])
		store.Put(entry.Key, entry)
	}

	return len(envelope.Entries), nil
}

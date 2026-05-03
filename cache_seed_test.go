package atropos_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	atropos "git.ucsc.edu/microfaults/atropos-go"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

func TestSeed_PopulatesStore(t *testing.T) {
	entries := []cachebox.WireEntry{
		{
			Key:               "exact:GET|/products",
			StatusCode:        200,
			Header:            map[string][]string{"Content-Type": {"application/json"}},
			Body:              []byte(`{"items":[]}`),
			ObservedLatencyUs: 5000,
			RecordedAt:        time.Now().UTC(),
		},
		{
			Key:               "exact:GET|/health",
			StatusCode:        200,
			Body:              []byte(`ok`),
			ObservedLatencyUs: 500,
			RecordedAt:        time.Now().UTC(),
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/cache/entries" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("service") != "cart" {
			t.Errorf("service param = %q", r.URL.Query().Get("service"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string][]cachebox.WireEntry{
			"entries": entries,
		})
	}))
	defer server.Close()

	store := cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 100})

	n, err := atropos.Seed(context.Background(), server.URL, "cart", store)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if n != 2 {
		t.Errorf("seeded = %d, want 2", n)
	}
	if store.Len() != 2 {
		t.Errorf("store.Len = %d, want 2", store.Len())
	}

	entry, ok := store.Get("exact:GET|/products")
	if !ok {
		t.Fatal("missing /products entry")
	}
	if entry.StatusCode != 200 {
		t.Errorf("StatusCode = %d", entry.StatusCode)
	}
	if entry.ObservedLatency != 5*time.Millisecond {
		t.Errorf("ObservedLatency = %v", entry.ObservedLatency)
	}
}

func TestSeed_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string][]cachebox.WireEntry{
			"entries": {},
		})
	}))
	defer server.Close()

	store := cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 100})
	n, err := atropos.Seed(context.Background(), server.URL, "cart", store)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if n != 0 {
		t.Errorf("seeded = %d, want 0", n)
	}
}

func TestSeed_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer server.Close()

	store := cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 100})
	_, err := atropos.Seed(context.Background(), server.URL, "cart", store)
	if err == nil {
		t.Fatal("expected error for server 500")
	}
}

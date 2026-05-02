package atropos_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atropos "atropos-go"
	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

func TestCachePushClient_BatchByCount(t *testing.T) {
	var received atomic.Int32
	var mu sync.Mutex
	var lastBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		lastBody = b
		mu.Unlock()
		received.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 3,
		MaxWait:  10 * time.Second,
	})
	defer client.Stop()

	pushFn := client.PushFunc()
	for i := 0; i < 3; i++ {
		pushFn("key", &cachebox.Entry{
			Key: "key", StatusCode: 200, Body: []byte("body"),
		})
	}

	time.Sleep(200 * time.Millisecond)

	if received.Load() != 1 {
		t.Fatalf("expected 1 POST, got %d", received.Load())
	}

	var envelope struct {
		Service  string            `json:"service"`
		Instance string            `json:"instance"`
		Entries  []json.RawMessage `json:"entries"`
	}
	mu.Lock()
	body := lastBody
	mu.Unlock()
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if envelope.Service != "cart" {
		t.Errorf("service = %q", envelope.Service)
	}
	if len(envelope.Entries) != 3 {
		t.Errorf("entries = %d, want 3", len(envelope.Entries))
	}
}

func TestCachePushClient_BatchByTime(t *testing.T) {
	var received atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 100,
		MaxWait:  200 * time.Millisecond,
	})
	defer client.Stop()

	client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x")})

	time.Sleep(500 * time.Millisecond)

	if received.Load() != 1 {
		t.Fatalf("expected 1 POST after time flush, got %d", received.Load())
	}
}

func TestCachePushClient_StopFlushes(t *testing.T) {
	var received atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 100,
		MaxWait:  10 * time.Second,
	})

	client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x")})
	client.Stop()

	if received.Load() != 1 {
		t.Fatalf("Stop should flush pending batch, got %d POSTs", received.Load())
	}
}

func TestCachePushClient_DropOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 1,
		MaxWait:  10 * time.Second,
	})
	defer client.Stop()

	client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x")})

	time.Sleep(200 * time.Millisecond)

	if client.Stats().Dropped == 0 {
		t.Error("expected dropped count > 0 on server error")
	}
}

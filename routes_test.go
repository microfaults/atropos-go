package atropos

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestManteionClient_Register_IncludesRoutes(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sdk/register" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(RegisterResponse{Status: "registered"})
	}))
	defer srv.Close()

	c := &manteionClient{
		cfg: manteionConfig{
			url:         srv.URL,
			serviceName: "cartservice",
			instanceID:  "pod-1",
			routes: []Route{
				{Method: "GET", Path: "/cart/{user_id}", Description: "view cart"},
				{Method: "POST", Path: "/cart/{user_id}/items"},
			},
			pollInterval: 10 * time.Second,
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    newTestTargets(),
		logger:     slog.New(slog.DiscardHandler),
	}
	if err := c.register(t.Context()); err != nil {
		t.Fatalf("register: %v", err)
	}

	var req RegisterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("decode register body: %v", err)
	}
	want := []Route{
		{Method: "GET", Path: "/cart/{user_id}", Description: "view cart"},
		{Method: "POST", Path: "/cart/{user_id}/items"},
	}
	if !reflect.DeepEqual(req.Routes, want) {
		t.Fatalf("register body routes = %+v, want %+v", req.Routes, want)
	}
}

func TestManteionClient_Register_OmitsRoutesWhenEmpty(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(RegisterResponse{Status: "registered"})
	}))
	defer srv.Close()

	c := &manteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "grpc-svc", instanceID: "pod-2"},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    newTestTargets(),
		logger:     slog.New(slog.DiscardHandler),
	}
	if err := c.register(t.Context()); err != nil {
		t.Fatalf("register: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode register body: %v", err)
	}
	if _, ok := raw["routes"]; ok {
		t.Fatalf("register body should omit routes when none supplied, got %s", body)
	}
}

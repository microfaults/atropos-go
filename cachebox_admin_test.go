package atropos

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCacheBoxAdminHandler(t *testing.T) {
	newCB := func(t *testing.T) *CacheBox {
		t.Helper()
		cb := NewCacheBox(CacheBoxConfig{
			Store: NewCacheBoxMemStore(100),
		})
		t.Cleanup(cb.Stop)
		return cb
	}

	t.Run("GET returns 200 with store and recorder stats", func(t *testing.T) {
		cb := newCB(t)
		handler := CacheBoxAdminHandler(cb)

		req := httptest.NewRequest(http.MethodGet, "/admin/cachebox", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		// Stats fields have no json tags, so Go encodes with verbatim exported names.
		var got struct {
			Store    map[string]any `json:"Store"`
			Recorder map[string]any `json:"Recorder"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if got.Store == nil {
			t.Fatal("expected Store key in response")
		}
		if got.Recorder == nil {
			t.Fatal("expected Recorder key in response")
		}
	})

	t.Run("POST /delay with valid params returns 204", func(t *testing.T) {
		cb := newCB(t)
		handler := CacheBoxAdminHandler(cb)

		body := `{"mu":1.0,"sigma":0.5,"seed":42}`
		req := httptest.NewRequest(http.MethodPost, "/admin/cachebox/delay", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("POST /delay with negative sigma returns 400", func(t *testing.T) {
		cb := newCB(t)
		handler := CacheBoxAdminHandler(cb)

		body := `{"mu":1.0,"sigma":-1.0,"seed":42}`
		req := httptest.NewRequest(http.MethodPost, "/admin/cachebox/delay", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("DELETE returns 204 and clears the store", func(t *testing.T) {
		cb := newCB(t)
		handler := CacheBoxAdminHandler(cb)

		req := httptest.NewRequest(http.MethodDelete, "/admin/cachebox", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
		}
		if cb.Stats().Store.Entries != 0 {
			t.Fatalf("expected store to be empty after DELETE, got %d entries", cb.Stats().Store.Entries)
		}
	})

	t.Run("PUT returns 405", func(t *testing.T) {
		cb := newCB(t)
		handler := CacheBoxAdminHandler(cb)

		req := httptest.NewRequest(http.MethodPut, "/admin/cachebox", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

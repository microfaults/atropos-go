package atropos

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
)

func TestRulesAdminHandler(t *testing.T) {
	t.Run("GET on empty evaluator returns 200 with empty array", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(eval)

		req := httptest.NewRequest(http.MethodGet, "/admin/rules", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var got []StaticRule
		if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty rule list, got %d rules", len(got))
		}
	})

	t.Run("POST with valid compiled rules replaces evaluator rules", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(eval)

		compiled := CompiledRule{
			Name:           "freeze-productcatalog",
			InjectionPoint: "egress",
			Mode:           "inline",
			CacheBox:       &CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"},
		}
		body, err := json.Marshal([]CompiledRule{compiled})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
		}

		rules := eval.Rules()
		if len(rules) != 1 {
			t.Fatalf("expected 1 rule after POST, got %d", len(rules))
		}
		if rules[0].Name != compiled.Name {
			t.Fatalf("expected rule name %q, got %q", compiled.Name, rules[0].Name)
		}
		if rules[0].Point != evaluator.Egress {
			t.Fatalf("expected rule point %v, got %v", evaluator.Egress, rules[0].Point)
		}
		if rules[0].Decision.CacheBox != evaluator.CacheBoxReplay {
			t.Fatalf("expected evaluator.CacheBoxReplay, got %v", rules[0].Decision.CacheBox)
		}
	})

	t.Run("POST with malformed JSON returns 400", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(eval)

		req := httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader([]byte(`"not json"`)))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("DELETE returns 405", func(t *testing.T) {
		eval := evaluator.NewStaticEvaluator()
		handler := rulesAdminHandler(eval)

		req := httptest.NewRequest(http.MethodDelete, "/admin/rules", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

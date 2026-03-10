package rule_engine

import (
	"os"
	"path/filepath"
	"testing"
)

// ─── Integration tests (real OPA) ───────────────────────────────────

func TestEngine_SimpleRule(t *testing.T) {
	path := filepath.Join("example_rules", "simple_rule.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	g := engine.Graph()
	t.Logf("graph: %d nodes, %d edges", g.NodeCount(), g.EdgeCount())

	// Request that matches both conditions: service=checkout, total > 100.
	results, err := engine.Evaluate(map[string]any{
		"service":      "checkout",
		"total_amount": map[string]any{"units": float64(200)},
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 fault result, got %d", len(results))
	}
	if results[0].Spec.FaultType != FaultCPU {
		t.Errorf("expected cpu fault, got %q", results[0].Spec.FaultType)
	}
	t.Logf("matched path: %v", results[0].MatchedPath)
}

func TestEngine_SimpleRule_NoMatch(t *testing.T) {
	path := filepath.Join("example_rules", "simple_rule.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	// Service matches but amount is too low → should short-circuit at high_value.
	results, err := engine.Evaluate(map[string]any{
		"service":      "checkout",
		"total_amount": map[string]any{"units": float64(50)},
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results (too low amount), got %d", len(results))
	}
}

func TestEngine_SimpleRule_WrongService(t *testing.T) {
	path := filepath.Join("example_rules", "simple_rule.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	// Different service → root condition fails, entire subtree pruned.
	results, err := engine.Evaluate(map[string]any{
		"service":      "inventory",
		"total_amount": map[string]any{"units": float64(500)},
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results (wrong service), got %d", len(results))
	}
}

func TestEngine_ECommerceRules_BlacklistedUser(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	g := engine.Graph()
	t.Logf("e-commerce graph: %d nodes, %d edges", g.NodeCount(), g.EdgeCount())

	// Blacklisted user on checkout service.
	results, err := engine.Evaluate(map[string]any{
		"service": "checkout",
		"user_id": "hacker1",
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	found := false
	for _, r := range results {
		if r.Spec.FaultType == FaultCPU && r.Spec.InjectionPoint == InjectInbound {
			found = true
			t.Logf("blacklisted user matched: path=%v", r.MatchedPath)
		}
	}
	if !found {
		t.Error("expected CPU fault for blacklisted user on checkout, not found")
	}
}

func TestEngine_ECommerceRules_InternationalPayment(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	// International high-value payment.
	results, err := engine.Evaluate(map[string]any{
		"service":          "payment",
		"total_amount":     map[string]any{"units": float64(750)},
		"shipping_country": "DE",
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	found := false
	for _, r := range results {
		if r.Spec.FaultType == FaultLatency && r.Spec.TargetService == "payment-gateway" {
			found = true
			t.Logf("international payment matched: path=%v", r.MatchedPath)
		}
	}
	if !found {
		t.Error("expected latency fault for international payment, not found")
	}
}

func TestEngine_ECommerceRules_GuestCart(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	// Guest user on cart service.
	results, err := engine.Evaluate(map[string]any{
		"service": "cart",
		"user_id": "",
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	found := false
	for _, r := range results {
		if r.Spec.FaultType == FaultPacketDrop {
			found = true
			t.Logf("guest cart drop matched: path=%v", r.MatchedPath)
		}
	}
	if !found {
		t.Error("expected packet_drop fault for guest cart, not found")
	}
}

func TestEngine_ECommerceRules_NoMatch(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	// Completely benign request — should match no faults.
	results, err := engine.Evaluate(map[string]any{
		"service": "search",
		"user_id": "normal_user",
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for benign request, got %d", len(results))
	}
}

func TestEngine_ECommerceRules_LowStockInventory(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	results, err := engine.Evaluate(map[string]any{
		"service":        "inventory",
		"stock_quantity": float64(3),
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	found := false
	for _, r := range results {
		if r.Spec.FaultType == FaultIO {
			found = true
			t.Logf("low stock matched: path=%v", r.MatchedPath)
		}
	}
	if !found {
		t.Error("expected IO fault for low stock inventory, not found")
	}
}

func TestEngine_ECommerceRules_RegionalShipping(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	engine, err := NewEngineFromFile(path)
	if err != nil {
		t.Fatalf("NewEngineFromFile: %v", err)
	}

	results, err := engine.Evaluate(map[string]any{
		"service": "shipping",
		"region":  "eu-west-1",
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	found := false
	for _, r := range results {
		if r.Spec.FaultType == FaultLatency && r.Spec.TargetService == "shipping-provider-api" {
			found = true
			t.Logf("regional shipping matched: path=%v", r.MatchedPath)
		}
	}
	if !found {
		t.Error("expected latency fault for regional shipping, not found")
	}
}

func TestEngine_FromDir(t *testing.T) {
	// Use a temp directory with only e_commerce_rules.json + its rego files
	// to avoid duplicate node IDs (simple_rule.json also has checkout_svc).
	dir := t.TempDir()

	// Copy the e_commerce rules JSON.
	src := filepath.Join("example_rules", "e_commerce_rules.json")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "e_commerce_rules.json"), data, 0644); err != nil {
		t.Fatalf("write dest: %v", err)
	}

	// Copy all .rego files referenced by e_commerce_rules.
	regoFiles := []string{
		"checkout_svc.rego",
		"payment_svc.rego",
		"international_origin.rego",
		"low_stock.rego",
		"target_region.rego",
	}
	for _, rf := range regoFiles {
		data, err := os.ReadFile(filepath.Join("example_rules", rf))
		if err != nil {
			t.Fatalf("read rego %s: %v", rf, err)
		}
		if err := os.WriteFile(filepath.Join(dir, rf), data, 0644); err != nil {
			t.Fatalf("write rego %s: %v", rf, err)
		}
	}

	engine, err := NewEngineFromDir(dir)
	if err != nil {
		t.Fatalf("NewEngineFromDir: %v", err)
	}

	g := engine.Graph()
	t.Logf("full dir graph: %d nodes, %d edges", g.NodeCount(), g.EdgeCount())

	if g.NodeCount() < 15 {
		t.Errorf("expected at least 15 nodes from dir load, got %d", g.NodeCount())
	}
}

func TestEngine_FromDir_DuplicateIDs(t *testing.T) {
	// Loading example_rules/ directly has conflicting checkout_svc IDs
	// across simple_rule.json and e_commerce_rules; should error.
	_, err := NewEngineFromDir("example_rules")
	if err == nil {
		t.Fatal("expected error for duplicate node IDs across rule files")
	}
	t.Logf("got expected error: %v", err)
}

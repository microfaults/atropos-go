package rule_engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRuleFile(t *testing.T) {
	path := filepath.Join("example_rules", "simple_rule.json")
	rule, err := LoadRuleFile(path)
	if err != nil {
		t.Fatalf("LoadRuleFile: %v", err)
	}
	if rule.ID != "checkout_high_value" {
		t.Errorf("expected id 'checkout_high_value', got %q", rule.ID)
	}
	if len(rule.Conditions) != 2 {
		t.Errorf("expected 2 conditions, got %d", len(rule.Conditions))
	}
	if len(rule.Faults) != 1 {
		t.Errorf("expected 1 fault, got %d", len(rule.Faults))
	}
	if len(rule.Edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(rule.Edges))
	}
}

func TestLoadRulesFile_Array(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	rules, err := LoadRulesFile(path)
	if err != nil {
		t.Fatalf("LoadRulesFile: %v", err)
	}
	if len(rules) != 5 {
		t.Fatalf("expected 5 rules, got %d", len(rules))
	}

	ids := map[string]bool{}
	for _, r := range rules {
		ids[r.ID] = true
	}
	expected := []string{
		"blacklisted_checkout",
		"high_value_international_payment",
		"low_stock_inventory",
		"guest_cart_drops",
		"regional_shipping_latency",
	}
	for _, id := range expected {
		if !ids[id] {
			t.Errorf("missing rule id %q", id)
		}
	}
}

func TestLoadFile_AutoDetect_SingleObject(t *testing.T) {
	path := filepath.Join("example_rules", "simple_rule.json")
	rules, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile (single): %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule from single-object file, got %d", len(rules))
	}
	if rules[0].ID != "checkout_high_value" {
		t.Errorf("unexpected id: %q", rules[0].ID)
	}
}

func TestLoadFile_AutoDetect_Array(t *testing.T) {
	path := filepath.Join("example_rules", "e_commerce_rules.json")
	rules, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile (array): %v", err)
	}
	if len(rules) != 5 {
		t.Fatalf("expected 5 rules from array file, got %d", len(rules))
	}
}

func TestLoadRuleDir(t *testing.T) {
	rules, err := LoadRuleDir("example_rules")
	if err != nil {
		t.Fatalf("LoadRuleDir: %v", err)
	}
	// simple_rule.json (1) + e_commerce_rules.json (5) = 6
	if len(rules) != 6 {
		t.Fatalf("expected 6 rules from dir, got %d", len(rules))
	}
}

func TestLoadRuleFile_NotFound(t *testing.T) {
	_, err := LoadRuleFile("nonexistent.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadRuleBytes_InvalidJSON(t *testing.T) {
	_, err := LoadRuleBytes([]byte("{invalid"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLoadRuleBytes_MissingID(t *testing.T) {
	_, err := LoadRuleBytes([]byte(`{"name":"no id"}`))
	if err == nil {
		t.Fatal("expected error for missing id")
	}
}

func TestLoadRuleDir_NotFound(t *testing.T) {
	_, err := LoadRuleDir("nonexistent_dir")
	if err == nil {
		t.Fatal("expected error for missing dir")
	}
}

func TestLoadRuleDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadRuleDir(dir)
	if err == nil {
		t.Fatal("expected error for empty dir")
	}
}

func TestLoadFile_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	os.WriteFile(path, []byte(""), 0644)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

// ─── Match / rego_file resolution tests ─────────────────────────────

func TestConditionDef_MatchRules(t *testing.T) {
	// Verify that match rules are auto-compiled to Rego via resolveRego.
	cond := ConditionDef{
		ID:   "match_test",
		Name: "Match Test",
		Match: []MatchRule{
			{Field: "user_id", Operator: "==", Value: "xyz"},
			{Field: "total", Operator: ">", Value: float64(100)},
		},
	}

	rego, err := resolveRego(cond, "")
	if err != nil {
		t.Fatalf("resolveRego (match): %v", err)
	}
	if rego == "" {
		t.Fatal("expected non-empty Rego from match rules")
	}
	t.Logf("Generated Rego:\n%s", rego)
}

func TestConditionDef_RegoFile(t *testing.T) {
	cond := ConditionDef{
		ID:       "rego_file_test",
		Name:     "Rego File Test",
		RegoFile: "checkout_svc.rego",
	}

	rego, err := resolveRego(cond, "example_rules")
	if err != nil {
		t.Fatalf("resolveRego (rego_file): %v", err)
	}
	if rego == "" {
		t.Fatal("expected non-empty Rego from rego_file")
	}
	t.Logf("Loaded Rego:\n%s", rego)
}

func TestConditionDef_InlineRego(t *testing.T) {
	cond := ConditionDef{
		ID:   "inline_test",
		Name: "Inline Test",
		Rego: "package inline_test\nimport rego.v1\ndefault match := false\nmatch if { true }",
	}

	rego, err := resolveRego(cond, "")
	if err != nil {
		t.Fatalf("resolveRego (inline): %v", err)
	}
	if rego != cond.Rego {
		t.Errorf("expected inline rego to be returned as-is")
	}
}

func TestConditionDef_NoneProvided(t *testing.T) {
	cond := ConditionDef{
		ID:   "empty",
		Name: "Empty",
	}

	_, err := resolveRego(cond, "")
	if err == nil {
		t.Fatal("expected error when no rego source is provided")
	}
}

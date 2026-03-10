package rule_engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// LoadRuleBytes unmarshals a single RuleDef from raw JSON bytes.
func LoadRuleBytes(data []byte) (RuleDef, error) {
	var r RuleDef
	if err := json.Unmarshal(data, &r); err != nil {
		return RuleDef{}, fmt.Errorf("load rule: %w", err)
	}
	if r.ID == "" {
		return RuleDef{}, fmt.Errorf("load rule: missing 'id' field")
	}
	return r, nil
}

// LoadRulesBytes unmarshals a JSON array of RuleDefs from raw bytes.
func LoadRulesBytes(data []byte) ([]RuleDef, error) {
	var rules []RuleDef
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("load rules: %w", err)
	}
	for i, r := range rules {
		if r.ID == "" {
			return nil, fmt.Errorf("load rules: rule at index %d missing 'id' field", i)
		}
	}
	return rules, nil
}

// LoadRuleFile reads a single JSON file and returns one RuleDef.
// The file must contain a single JSON object (not an array).
func LoadRuleFile(path string) (RuleDef, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RuleDef{}, fmt.Errorf("load rule file %q: %w", path, err)
	}
	return LoadRuleBytes(data)
}

// LoadRulesFile reads a JSON file that contains an array of RuleDefs
// and returns them all.
func LoadRulesFile(path string) ([]RuleDef, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load rules file %q: %w", path, err)
	}
	return LoadRulesBytes(data)
}

// LoadFile auto-detects whether the JSON file contains a single
// RuleDef object or an array of RuleDefs, and returns a slice either
// way.  This is the recommended loader for user-facing code.
func LoadFile(path string) ([]RuleDef, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load file %q: %w", path, err)
	}

	// Peek at the first non-whitespace byte to decide format.
	for _, b := range data {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		case '[':
			return LoadRulesBytes(data)
		case '{':
			r, err := LoadRuleBytes(data)
			if err != nil {
				return nil, err
			}
			return []RuleDef{r}, nil
		default:
			return nil, fmt.Errorf("load file %q: unexpected JSON start character %q", path, string(b))
		}
	}
	return nil, fmt.Errorf("load file %q: empty file", path)
}

// LoadRuleDir scans a directory for *.json files and returns all
// successfully parsed RuleDefs.  Non-JSON files are silently skipped.
// Each JSON file may contain a single RuleDef or an array of RuleDefs.
func LoadRuleDir(dir string) ([]RuleDef, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("load rule dir %q: %w", dir, err)
	}

	var rules []RuleDef
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		loaded, err := LoadFile(path)
		if err != nil {
			return nil, fmt.Errorf("load rule dir: %w", err)
		}
		rules = append(rules, loaded...)
	}

	if len(rules) == 0 {
		return nil, fmt.Errorf("load rule dir %q: no rule JSON files found", dir)
	}

	return rules, nil
}

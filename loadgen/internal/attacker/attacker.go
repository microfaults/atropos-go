package attacker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"

	"github.com/microfaults/atropos-go/loadgen/internal/dedup"
	"github.com/microfaults/atropos-go/loadgen/internal/trace"
)

// TargetSpec defines the HTTP target for an attack.
type TargetSpec struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

// AttackConfig defines a single targeted attack.
type AttackConfig struct {
	ID          string        `json:"id"`
	Target      TargetSpec    `json:"target"`
	Rate        int           `json:"rate"`
	Duration    time.Duration `json:"duration"`
	WorkloadRef string        `json:"workload_ref,omitempty"`
	DedupBypass string        `json:"dedup_bypass,omitempty"`
	MetaTraceID string        `json:"meta_trace_id,omitempty"`
}

// Validate checks the attack config before execution.
func (c *AttackConfig) Validate() error {
	if c.Target.URL == "" {
		return fmt.Errorf("attacker: target url is required")
	}
	if c.Rate <= 0 {
		return fmt.Errorf("attacker: rate must be > 0, got %d", c.Rate)
	}
	if c.Duration <= 0 {
		return fmt.Errorf("attacker: duration must be > 0, got %s", c.Duration)
	}
	if c.Target.Method == "" {
		c.Target.Method = http.MethodGet
	}
	return nil
}

// Attack represents a running or completed vegeta attack.
type Attack struct {
	Config    AttackConfig  `json:"config"`
	Status    string        `json:"status"`
	StartedAt time.Time    `json:"started_at"`
	Result    *AttackResult `json:"result,omitempty"`
	cancel    context.CancelFunc
}

// Manager orchestrates concurrent attacks with lifecycle tracking.
type Manager struct {
	mu       sync.RWMutex
	attacks  map[string]*Attack
	bypasses map[string]dedup.DedupBypass
}

// NewManager creates an attack manager.
func NewManager() *Manager {
	return &Manager{
		attacks:  make(map[string]*Attack),
		bypasses: make(map[string]dedup.DedupBypass),
	}
}

// RegisterBypass registers a named dedup bypass strategy.
func (m *Manager) RegisterBypass(name string, b dedup.DedupBypass) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bypasses[name] = b
}

// Launch starts an attack in the background and returns immediately.
func (m *Manager) Launch(ctx context.Context, cfg AttackConfig) (*Attack, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration+5*time.Second)

	attack := &Attack{
		Config:    cfg,
		Status:    "running",
		StartedAt: time.Now(),
		cancel:    cancel,
	}

	m.mu.Lock()
	m.attacks[cfg.ID] = attack
	bypass := m.bypasses[cfg.DedupBypass]
	m.mu.Unlock()

	targeter := m.buildTargeter(cfg, bypass)
	pacer := vegeta.ConstantPacer{Freq: cfg.Rate, Per: time.Second}

	atk := vegeta.NewAttacker()

	go func() {
		defer cancel()
		results := atk.Attack(targeter, pacer, cfg.Duration, cfg.ID)

		var metrics vegeta.Metrics
		for r := range results {
			metrics.Add(r)
		}
		metrics.Close()

		m.mu.Lock()
		attack.Result = FromMetrics(&metrics)
		if ctx.Err() != nil && attack.Status == "running" {
			attack.Status = "stopped"
		} else if attack.Status == "running" {
			attack.Status = "completed"
		}
		m.mu.Unlock()
	}()

	return attack, nil
}

// Get returns an attack by ID.
func (m *Manager) Get(id string) (*Attack, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.attacks[id]
	return a, ok
}

// Stop cancels a running attack.
func (m *Manager) Stop(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.attacks[id]
	if !ok {
		return fmt.Errorf("attacker: attack %q not found", id)
	}
	if a.Status != "running" {
		return fmt.Errorf("attacker: attack %q is not running (status: %s)", id, a.Status)
	}
	a.Status = "stopped"
	a.cancel()
	return nil
}

// List returns all tracked attacks.
func (m *Manager) List() []*Attack {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Attack, 0, len(m.attacks))
	for _, a := range m.attacks {
		out = append(out, a)
	}
	return out
}

// StopAll cancels all running attacks.
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.attacks {
		if a.Status == "running" {
			a.Status = "stopped"
			a.cancel()
		}
	}
}

func (m *Manager) buildTargeter(cfg AttackConfig, bypass dedup.DedupBypass) vegeta.Targeter {
	base := vegeta.Target{
		Method: cfg.Target.Method,
		URL:    cfg.Target.URL,
		Body:   cfg.Target.Body,
		Header: toHTTPHeader(cfg.Target.Headers),
	}

	return func(t *vegeta.Target) error {
		*t = base
		// Deep copy header to avoid concurrent mutation.
		t.Header = base.Header.Clone()

		if bypass != nil {
			req, err := http.NewRequest(t.Method, t.URL, nil)
			if err != nil {
				return err
			}
			req.Header = t.Header
			transformed := bypass.Transform(req)
			t.Header = transformed.Header
			t.URL = transformed.URL.String()
			if transformed.Body != nil {
				body, err := io.ReadAll(transformed.Body)
				if err != nil {
					return err
				}
				t.Body = body
			}
		}

		if cfg.MetaTraceID != "" {
			trace.InjectBaggageHeader(t.Header, cfg.MetaTraceID)
		}

		return nil
	}
}

func toHTTPHeader(headers map[string]string) http.Header {
	h := make(http.Header, len(headers))
	for k, v := range headers {
		h.Set(k, v)
	}
	return h
}

// LogAttackResult logs a summary of attack results.
func LogAttackResult(id string, a *Attack) {
	if a.Result == nil {
		log.Printf("attack %s: no results yet (status: %s)", id, a.Status)
		return
	}
	log.Printf("attack %s: %d reqs, %.1f req/s, p50=%s p99=%s, %.0f%% success",
		id, a.Result.TotalRequests, a.Result.RateActual,
		a.Result.Latencies.P50, a.Result.Latencies.P99,
		a.Result.Success*100)
}

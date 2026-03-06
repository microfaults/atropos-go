package workload

import (
	"fmt"
	"sync"
	"time"
)

// Workload represents an active k6 load generation workload.
type Workload struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Profile      string   `json:"profile"`
	Targets      []string `json:"targets"`
	VUs          int      `json:"vus"`
	Rate         float64  `json:"rate"`
	MetaTraceID  string   `json:"meta_trace_id"`
	Status       string   `json:"status"`
	RegisteredAt time.Time `json:"registered_at"`
}

// Validate checks that the workload has required fields.
func (w *Workload) Validate() error {
	if w.ID == "" {
		return fmt.Errorf("workload: id is required")
	}
	if len(w.Targets) == 0 {
		return fmt.Errorf("workload: at least one target is required")
	}
	if w.VUs <= 0 {
		return fmt.Errorf("workload: vus must be > 0, got %d", w.VUs)
	}
	return nil
}

// Registry is a thread-safe store for active workloads.
type Registry struct {
	mu        sync.RWMutex
	workloads map[string]*Workload
}

// NewRegistry creates an empty workload registry.
func NewRegistry() *Registry {
	return &Registry{
		workloads: make(map[string]*Workload),
	}
}

// Register adds a workload to the registry.
// Returns an error if the workload is invalid or its ID already exists.
func (r *Registry) Register(w *Workload) error {
	if err := w.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.workloads[w.ID]; exists {
		return fmt.Errorf("workload: id %q already registered", w.ID)
	}
	r.workloads[w.ID] = w
	return nil
}

// Get returns a workload by ID.
func (r *Registry) Get(id string) (*Workload, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.workloads[id]
	return w, ok
}

// List returns all registered workloads.
func (r *Registry) List() []*Workload {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Workload, 0, len(r.workloads))
	for _, w := range r.workloads {
		out = append(out, w)
	}
	return out
}

// Deregister removes a workload by ID.
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.workloads[id]; !exists {
		return fmt.Errorf("workload: id %q not found", id)
	}
	delete(r.workloads, id)
	return nil
}

// TargetsInUse returns the deduplicated union of all targets across active workloads.
func (r *Registry) TargetsInUse() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{})
	for _, w := range r.workloads {
		for _, t := range w.Targets {
			seen[t] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	return out
}

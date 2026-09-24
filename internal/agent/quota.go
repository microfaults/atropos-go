package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// QuotaKind selects the cgroup limit a quota lowers.
type QuotaKind string

const (
	QuotaCPU    QuotaKind = "cpu"    // cpu.max
	QuotaMemory QuotaKind = "memory" // memory.high
)

// minCPUQuotaUs is the kernel's minimum cpu.max quota.
const minCPUQuotaUs = 1000

// Quota is one applied limit reduction, journaled before it is written so
// it can be restored after a stop, a crash, or an agent restart.
type Quota struct {
	ID        string    `json:"id"`
	Dir       string    `json:"dir"`
	Kind      QuotaKind `json:"kind"`
	File      string    `json:"file"`
	Original  string    `json:"original"`
	Applied   string    `json:"applied"`
	Fraction  float64   `json:"fraction"`
	AppliedAt time.Time `json:"applied_at"`
}

// Journal persists applied quotas as one JSON file each.
type Journal struct{ Dir string }

func (j Journal) path(id string) string { return filepath.Join(j.Dir, id+".json") }

// Save writes q atomically.
func (j Journal) Save(q Quota) error {
	if err := os.MkdirAll(j.Dir, 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(q)
	if err != nil {
		return err
	}
	tmp := j.path(q.ID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, j.path(q.ID))
}

// Remove deletes q's journal entry.
func (j Journal) Remove(id string) error {
	err := os.Remove(j.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Load reads every journaled quota.
func (j Journal) Load() ([]Quota, error) {
	entries, err := os.ReadDir(j.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Quota
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(j.Dir, e.Name()))
		if err != nil {
			continue
		}
		var q Quota
		if json.Unmarshal(b, &q) == nil && q.File != "" {
			out = append(out, q)
		}
	}
	return out, nil
}

// ApplyQuota lowers the cgroup limit of kind in dir by fraction of its
// current capacity (a target_load of 0.5 removes half), journaling the
// original value first. cpu: cpu.max quota; memory: memory.high from
// memory.max (reclaim pressure, not an OOM ceiling).
func ApplyQuota(j Journal, id, dir string, kind QuotaKind, fraction float64, host Capacity) (*Quota, error) {
	if fraction <= 0 || fraction >= 1 {
		return nil, fmt.Errorf("agent: quota fraction %v must be in (0,1)", fraction)
	}
	q := &Quota{ID: id, Dir: dir, Kind: kind, Fraction: fraction, AppliedAt: time.Now()}
	switch kind {
	case QuotaCPU:
		q.File = filepath.Join(dir, "cpu.max")
		orig, err := os.ReadFile(q.File)
		if err != nil {
			return nil, err
		}
		quota, period, err := ReadCPUMax(dir)
		if err != nil {
			return nil, err
		}
		if quota < 0 {
			quota = int64(host.CPUs * float64(period))
		}
		nq := int64(float64(quota) * (1 - fraction))
		if nq < minCPUQuotaUs {
			nq = minCPUQuotaUs
		}
		q.Original = strings.TrimSpace(string(orig))
		q.Applied = fmt.Sprintf("%d %d", nq, period)
	case QuotaMemory:
		q.File = filepath.Join(dir, "memory.high")
		orig, err := os.ReadFile(q.File)
		if err != nil {
			return nil, err
		}
		max, err := ReadMemoryMax(dir)
		if err != nil {
			return nil, err
		}
		if max < 0 {
			max = int64(host.MemoryBytes)
		}
		q.Original = strings.TrimSpace(string(orig))
		q.Applied = strconv.FormatInt(int64(float64(max)*(1-fraction)), 10)
	default:
		return nil, fmt.Errorf("agent: quota is not supported for %q (cpu|memory)", kind)
	}
	if err := j.Save(*q); err != nil {
		return nil, fmt.Errorf("agent: journal quota: %w", err)
	}
	if err := os.WriteFile(q.File, []byte(q.Applied+"\n"), 0o644); err != nil {
		_ = j.Remove(id)
		return nil, fmt.Errorf("agent: apply quota: %w", err)
	}
	return q, nil
}

// RestoreQuota writes the original value back and drops the journal entry.
func RestoreQuota(j Journal, q Quota) error {
	if err := os.WriteFile(q.File, []byte(q.Original+"\n"), 0o644); err != nil {
		return fmt.Errorf("agent: restore %s: %w", q.File, err)
	}
	return j.Remove(q.ID)
}

// RestoreAll restores every journaled quota (agent boot): a quota must never
// outlive the agent that applied it. Returns how many were restored.
func RestoreAll(j Journal, logger *slog.Logger) int {
	qs, err := j.Load()
	if err != nil {
		logger.Warn("agent: read quota journal", "error", err)
		return 0
	}
	n := 0
	for _, q := range qs {
		if err := RestoreQuota(j, q); err != nil {
			logger.Warn("agent: restore journaled quota", "id", q.ID, "file", q.File, "error", err)
			continue
		}
		n++
		logger.Info("agent: restored journaled quota", "id", q.ID, "file", q.File, "original", q.Original)
	}
	return n
}

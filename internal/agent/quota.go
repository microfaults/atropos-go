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

func (j Journal) path(id string) string { return journalPath(j.Dir, id) }

// Save writes q atomically.
func (j Journal) Save(q Quota) error { return writeJournalFile(j.Dir, q.ID, q) }

// Remove deletes q's journal entry.
func (j Journal) Remove(id string) error { return removeJournalFile(j.Dir, id) }

// Load reads every journaled quota.
func (j Journal) Load() ([]Quota, error) {
	files, err := readJournalFiles(j.Dir)
	if err != nil {
		return nil, err
	}
	var out []Quota
	for _, b := range files {
		var q Quota
		if json.Unmarshal(b, &q) == nil && q.File != "" {
			out = append(out, q)
		}
	}
	return out, nil
}

// The journal-file helpers shared by Journal and NetemJournal: one JSON file
// per entry, <dir>/<id>.json, written through a tmp file and a rename so a
// crash never leaves a torn entry.

func journalPath(dir, id string) string { return filepath.Join(dir, id+".json") }

// writeJournalFile writes v as <dir>/<id>.json atomically, creating dir.
func writeJournalFile(dir, id string, v any) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := journalPath(dir, id) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, journalPath(dir, id))
}

// removeJournalFile deletes <dir>/<id>.json; a missing entry is not an error.
func removeJournalFile(dir, id string) error {
	err := os.Remove(journalPath(dir, id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// readJournalFiles returns the contents of every *.json file in dir (a
// missing dir reads as empty; an unreadable file is skipped).
func readJournalFiles(dir string) ([][]byte, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		out = append(out, b)
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

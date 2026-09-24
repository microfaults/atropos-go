package agent

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readTrim(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestApplyQuota_CPUAndMemoryWithJournalAndRestore(t *testing.T) {
	j := Journal{Dir: filepath.Join(t.TempDir(), "journal")}
	host := Capacity{CPUs: 2, MemoryBytes: 4 << 30}

	// Unlimited cpu.max: capacity = host CPUs × period; remove half.
	dir := cgroupDir(t, map[string]string{"cpu.max": "max 100000\n", "memory.max": "1073741824\n", "memory.high": "max\n"})
	q, err := ApplyQuota(j, "q1", dir, QuotaCPU, 0.5, host)
	if err != nil {
		t.Fatal(err)
	}
	if got := readTrim(t, filepath.Join(dir, "cpu.max")); got != "100000 100000" {
		t.Fatalf("cpu.max = %q, want 100000 100000 (2 CPUs × 0.5)", got)
	}
	if _, err := os.Stat(j.path("q1")); err != nil {
		t.Fatal("journal entry must exist while the quota is applied")
	}
	if err := RestoreQuota(j, *q); err != nil {
		t.Fatal(err)
	}
	if got := readTrim(t, filepath.Join(dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("cpu.max after restore = %q", got)
	}
	if _, err := os.Stat(j.path("q1")); !os.IsNotExist(err) {
		t.Fatal("journal entry must be removed after restore")
	}

	// Limited cpu.max: remove a quarter.
	os.WriteFile(filepath.Join(dir, "cpu.max"), []byte("200000 100000\n"), 0o644)
	if _, err := ApplyQuota(j, "q2", dir, QuotaCPU, 0.25, host); err != nil {
		t.Fatal(err)
	}
	if got := readTrim(t, filepath.Join(dir, "cpu.max")); got != "150000 100000" {
		t.Fatalf("cpu.max = %q, want 150000 100000", got)
	}

	// Memory: memory.high from memory.max.
	qm, err := ApplyQuota(j, "q3", dir, QuotaMemory, 0.25, host)
	if err != nil {
		t.Fatal(err)
	}
	if got := readTrim(t, filepath.Join(dir, "memory.high")); got != "805306368" {
		t.Fatalf("memory.high = %q, want 805306368 (1 GiB × 0.75)", got)
	}
	if err := RestoreQuota(j, *qm); err != nil || readTrim(t, filepath.Join(dir, "memory.high")) != "max" {
		t.Fatalf("memory.high restore: %v %q", err, readTrim(t, filepath.Join(dir, "memory.high")))
	}

	for _, f := range []float64{0, 1, 1.5, -0.1} {
		if _, err := ApplyQuota(j, "bad", dir, QuotaCPU, f, host); err == nil {
			t.Fatalf("fraction %v must be rejected", f)
		}
	}
	if _, err := ApplyQuota(j, "bad", dir, QuotaKind("io"), 0.5, host); err == nil {
		t.Fatal("io quota is not supported yet and must be rejected")
	}
}

func TestRestoreAll_RestoresJournaledQuotasAtBoot(t *testing.T) {
	j := Journal{Dir: filepath.Join(t.TempDir(), "journal")}
	dir := cgroupDir(t, map[string]string{"cpu.max": "max 100000\n"})
	if _, err := ApplyQuota(j, "crashed", dir, QuotaCPU, 0.5, Capacity{CPUs: 1}); err != nil {
		t.Fatal(err)
	}
	if got := readTrim(t, filepath.Join(dir, "cpu.max")); got != "50000 100000" {
		t.Fatalf("applied = %q", got)
	}
	// "Agent restarts": the journal is all it has.
	if n := RestoreAll(j, slog.New(slog.DiscardHandler)); n != 1 {
		t.Fatalf("restored %d, want 1", n)
	}
	if got := readTrim(t, filepath.Join(dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("cpu.max after boot restore = %q", got)
	}
	if qs, _ := j.Load(); len(qs) != 0 {
		t.Fatalf("journal must be empty after restore, got %d", len(qs))
	}
}

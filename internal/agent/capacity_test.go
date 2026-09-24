package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostCapacity_ReadsMemTotalFromProc(t *testing.T) {
	proc := t.TempDir()
	if err := os.WriteFile(filepath.Join(proc, "meminfo"), []byte("MemTotal:       16384000 kB\nMemFree:         1000000 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := HostCapacity(proc)
	if c.MemoryBytes != 16384000*1024 {
		t.Fatalf("MemoryBytes = %d, want MemTotal in bytes", c.MemoryBytes)
	}
	if c.CPUs <= 0 {
		t.Fatalf("CPUs = %v, want runtime.NumCPU()", c.CPUs)
	}
	if c := HostCapacity(filepath.Join(proc, "missing")); c.MemoryBytes != 0 || c.CPUs <= 0 {
		t.Fatalf("missing meminfo: %+v, want zero memory and the CPU count", c)
	}
}

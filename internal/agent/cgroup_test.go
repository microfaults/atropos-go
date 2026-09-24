package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cgroupDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCgroupReads(t *testing.T) {
	dir := cgroupDir(t, map[string]string{"cpu.max": "50000 100000\n", "memory.max": "1073741824\n"})
	if q, p, err := ReadCPUMax(dir); err != nil || q != 50000 || p != 100000 {
		t.Fatalf("cpu.max = %d %d %v", q, p, err)
	}
	if got := AvailableCPUs(dir, 8); got != 0.5 {
		t.Fatalf("AvailableCPUs = %v, want 0.5", got)
	}
	if got := AvailableMemory(dir, 1); got != 1073741824 {
		t.Fatalf("AvailableMemory = %d", got)
	}
	unl := cgroupDir(t, map[string]string{"cpu.max": "max 100000\n", "memory.max": "max\n"})
	if got := AvailableCPUs(unl, 8); got != 8 {
		t.Fatalf("unlimited cpu must fall back to host, got %v", got)
	}
	if got := AvailableMemory(unl, 42); got != 42 {
		t.Fatalf("unlimited memory must fall back to host, got %d", got)
	}
	if got := AvailableCPUs(t.TempDir(), 3); got != 3 {
		t.Fatalf("missing cpu.max must fall back to host, got %v", got)
	}
}

func TestAttachWritesPid(t *testing.T) {
	dir := cgroupDir(t, map[string]string{"cgroup.procs": ""})
	if err := Attach(dir, 4242); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
	if strings.TrimSpace(string(b)) != "4242" {
		t.Fatalf("cgroup.procs = %q", b)
	}
	if err := Attach(t.TempDir(), 1); err == nil {
		t.Fatal("attach without cgroup.procs must fail")
	}
}

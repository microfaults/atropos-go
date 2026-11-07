package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ReadCPUMax parses <dir>/cpu.max: "max 100000" or "50000 100000". quota
// is -1 when unlimited.
func ReadCPUMax(dir string) (quota, period int64, err error) {
	b, err := os.ReadFile(filepath.Join(dir, "cpu.max"))
	if err != nil {
		return 0, 0, err
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("agent: malformed cpu.max %q", strings.TrimSpace(string(b)))
	}
	period, err = strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if f[0] == "max" {
		return -1, period, nil
	}
	quota, err = strconv.ParseInt(f[0], 10, 64)
	return quota, period, err
}

// AvailableCPUs is the CPU capacity of a cgroup: quota/period, or host when
// unlimited or unreadable.
func AvailableCPUs(dir string, host float64) float64 {
	q, p, err := ReadCPUMax(dir)
	if err != nil || q < 0 || p <= 0 {
		return host
	}
	return float64(q) / float64(p)
}

// ReadMemoryMax parses <dir>/memory.max; -1 when "max".
func ReadMemoryMax(dir string) (int64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.max"))
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "max" {
		return -1, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// AvailableMemory is the memory limit of a cgroup, or host when unlimited
// or unreadable.
func AvailableMemory(dir string, host uint64) uint64 {
	m, err := ReadMemoryMax(dir)
	if err != nil || m < 0 {
		return host
	}
	return uint64(m)
}

// Attach moves pid into the cgroup (writes cgroup.procs).
func Attach(dir string, pid int) error {
	f, err := os.OpenFile(filepath.Join(dir, "cgroup.procs"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("agent: open cgroup.procs: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(strconv.Itoa(pid) + "\n"); err != nil {
		return fmt.Errorf("agent: attach pid %d to %s: %w", pid, dir, err)
	}
	return nil
}

// FirstPID is the first pid listed in <dir>/cgroup.procs; an error when the
// file is missing or lists no process.
func FirstPID(dir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
	if err != nil {
		return 0, fmt.Errorf("agent: no process in cgroup %s: %w", dir, err)
	}
	line, _, _ := strings.Cut(string(b), "\n")
	line = strings.TrimSpace(line)
	if line == "" {
		return 0, fmt.Errorf("agent: no process in cgroup %s", dir)
	}
	pid, err := strconv.Atoi(line)
	if err != nil {
		return 0, fmt.Errorf("agent: malformed cgroup.procs in %s: %w", dir, err)
	}
	return pid, nil
}

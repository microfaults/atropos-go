package memory

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// AvailableMemory returns the total memory limit in bytes for the current
// process, respecting container cgroup limits (Docker --memory, k8s
// resource limits).
//
// Detection order:
//  1. cgroup v2  →  /sys/fs/cgroup/memory.max
//  2. cgroup v1  →  /sys/fs/cgroup/memory/memory.limit_in_bytes
//  3. Linux      →  /proc/meminfo MemTotal
//  4. macOS      →  sysctl hw.memsize
//
// Returns 0 if detection fails entirely.
func AvailableMemory() uint64 {
	if mem := detectMemoryCgroupV2(); mem > 0 {
		return mem
	}
	if mem := detectMemoryCgroupV1(); mem > 0 {
		return mem
	}
	if mem := detectProcMeminfo(); mem > 0 {
		return mem
	}
	return detectSysctl()
}

// CurrentUsage returns the current memory usage in bytes for the process's
// cgroup. This is used to compute how much *additional* memory to allocate
// to reach the target load.
//
// Detection order:
//  1. cgroup v2  →  /sys/fs/cgroup/memory.current
//  2. cgroup v1  →  /sys/fs/cgroup/memory/memory.usage_in_bytes
//  3. Fallback   →  0 (assume negligible baseline)
func CurrentUsage() uint64 {
	if usage := readUint64File("/sys/fs/cgroup/memory.current"); usage > 0 {
		return usage
	}
	if usage := readUint64File("/sys/fs/cgroup/memory/memory.usage_in_bytes"); usage > 0 {
		return usage
	}
	return 0
}

// detectMemoryCgroupV2 reads /sys/fs/cgroup/memory.max.
// The file contains either a number (bytes) or "max" (unlimited).
func detectMemoryCgroupV2() uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0 // unlimited — fall through to next detector
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// detectMemoryCgroupV1 reads /sys/fs/cgroup/memory/memory.limit_in_bytes.
// Very large values (close to max int64) indicate no limit is set.
func detectMemoryCgroupV1() uint64 {
	v := readUint64File("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	// The kernel sets an astronomically large value when no limit is
	// configured (typically 2^63-4096 or similar). Treat anything above
	// 1 EiB as "unlimited".
	const oneEiB = uint64(1) << 60
	if v == 0 || v >= oneEiB {
		return 0
	}
	return v
}

// detectProcMeminfo parses /proc/meminfo for MemTotal (in kB).
func detectProcMeminfo() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kB, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kB * 1024 // convert kB → bytes
	}
	return 0
}

// detectSysctl uses `sysctl hw.memsize` to get total physical memory on
// macOS. This is a no-op on other platforms.
func detectSysctl() uint64 {
	if runtime.GOOS != "darwin" {
		return 0
	}
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// readUint64File reads a file containing a single uint64 value.
func readUint64File(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

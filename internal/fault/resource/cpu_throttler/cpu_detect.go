package fault

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// AvailableCPUs returns the number of CPUs available to the current process,
// respecting container cgroup limits (Docker --cpus, k8s resource limits).
//
// Detection order:
//  1. cgroup v2  →  /sys/fs/cgroup/cpu.max
//  2. cgroup v1  →  /sys/fs/cgroup/cpu/cpu.cfs_quota_us + period
//  3. Fallback   →  runtime.NumCPU()
//
// The result is a float64 because container limits can be fractional
// (e.g., --cpus=1.5 → 1.5).
func AvailableCPUs() float64 {
	if cpus := detectCgroupV2(); cpus > 0 {
		return cpus
	}
	if cpus := detectCgroupV1(); cpus > 0 {
		return cpus
	}
	return float64(runtime.NumCPU())
}

// detectCgroupV2 reads /sys/fs/cgroup/cpu.max (format: "$MAX $PERIOD" or "max $PERIOD").
func detectCgroupV2() float64 {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return 0
	}

	parts := strings.Fields(strings.TrimSpace(string(data)))
	if len(parts) != 2 || parts[0] == "max" {
		// "max" means unlimited — fall through to next detection.
		return 0
	}

	quota, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0
	}
	period, err := strconv.ParseFloat(parts[1], 64)
	if err != nil || period == 0 {
		return 0
	}

	return roundCPU(quota / period)
}

// detectCgroupV1 reads cpu.cfs_quota_us and cpu.cfs_period_us.
func detectCgroupV1() float64 {
	quotaData, err := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	if err != nil {
		return 0
	}
	periodData, err := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err != nil {
		return 0
	}

	quota, err := strconv.ParseFloat(strings.TrimSpace(string(quotaData)), 64)
	if err != nil {
		return 0
	}
	// -1 means no limit.
	if quota <= 0 {
		return 0
	}

	period, err := strconv.ParseFloat(strings.TrimSpace(string(periodData)), 64)
	if err != nil || period == 0 {
		return 0
	}

	return roundCPU(quota / period)
}

// roundCPU rounds to 2 decimal places to avoid floating-point noise.
func roundCPU(v float64) float64 {
	return math.Round(v*100) / 100
}

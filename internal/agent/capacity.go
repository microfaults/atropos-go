package agent

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// HostCapacity is the node's capacity as procfs reports it: the fallback for
// a target whose cgroup sets no limit. It deliberately ignores the agent's
// own cgroup, whose limits describe the agent container, not the node.
func HostCapacity(procRoot string) Capacity {
	c := Capacity{CPUs: float64(runtime.NumCPU())}
	f, err := os.Open(filepath.Join(procRoot, "meminfo"))
	if err != nil {
		return c
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				c.MemoryBytes = kb * 1024
			}
			break
		}
	}
	return c
}

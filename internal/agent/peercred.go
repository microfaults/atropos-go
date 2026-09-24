package agent

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// PeerPodUIDFunc identifies the pod a connecting process belongs to. The
// agent refuses any request for a different pod: a sidecar can only fault
// its own pod (design §5.14).
type PeerPodUIDFunc func(net.Conn) (string, error)

var podUIDRe = regexp.MustCompile(`pod([0-9a-f]{8})[-_]([0-9a-f]{4})[-_]([0-9a-f]{4})[-_]([0-9a-f]{4})[-_]([0-9a-f]{12})`)

// PodUIDFromCgroup extracts the pod UID from a /proc/<pid>/cgroup listing.
// Both the systemd driver's form (kubepods-burstable-pod<uid with _>.slice)
// and the cgroupfs driver's (/kubepods/burstable/pod<uid>) are recognised.
func PodUIDFromCgroup(content string) (string, error) {
	m := podUIDRe.FindStringSubmatch(content)
	if m == nil {
		return "", errors.New("agent: no pod uid in cgroup path (caller is not a pod process)")
	}
	return strings.Join(m[1:], "-"), nil
}

func podUIDFromProc(procRoot string, pid int) (string, error) {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", err
	}
	return PodUIDFromCgroup(string(b))
}

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Target is a resolved container: the cgroup directory a stress child is
// placed in or whose limits a quota lowers.
type Target struct {
	PodUID      string
	Container   string
	ContainerID string
	CgroupDir   string
}

// TargetResolver maps (pod uid, container name) to a Target.
type TargetResolver interface {
	Resolve(podUID, container string) (Target, error)
}

// DefaultTaskRoots are where containerd keeps the OCI runtime spec of every
// running container (config.json under <root>/k8s.io/<container id>/):
// upstream containerd, then k3s's embedded containerd.
var DefaultTaskRoots = []string{
	"/run/containerd/io.containerd.runtime.v2.task",
	"/run/k3s/containerd/io.containerd.runtime.v2.task",
}

// DefaultCgroupRoot is where the DaemonSet mounts the host's cgroup v2 root.
const DefaultCgroupRoot = "/host/sys/fs/cgroup"

// Resolver resolves containers from the filesystem alone: containerd's OCI
// specs carry the CRI annotations that name the pod and container, and the
// container id is in the name of its cgroup directory under kubepods.
// No CRI client, no gRPC dependency.
type Resolver struct {
	TaskRoots  []string
	CgroupRoot string
}

const (
	annContainerName = "io.kubernetes.cri.container-name"
	annSandboxUID    = "io.kubernetes.cri.sandbox-uid"
	annContainerType = "io.kubernetes.cri.container-type"
)

type ociSpec struct {
	Annotations map[string]string `json:"annotations"`
}

// Resolve implements TargetResolver.
func (r *Resolver) Resolve(podUID, container string) (Target, error) {
	roots := r.TaskRoots
	if len(roots) == 0 {
		roots = DefaultTaskRoots
	}
	for _, root := range roots {
		entries, err := os.ReadDir(filepath.Join(root, "k8s.io"))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			b, err := os.ReadFile(filepath.Join(root, "k8s.io", e.Name(), "config.json"))
			if err != nil {
				continue
			}
			var spec ociSpec
			if json.Unmarshal(b, &spec) != nil {
				continue
			}
			a := spec.Annotations
			if a[annSandboxUID] != podUID || a[annContainerName] != container || a[annContainerType] == "sandbox" {
				continue
			}
			dir, err := findCgroupDir(r.CgroupRoot, e.Name())
			if err != nil {
				return Target{}, err
			}
			return Target{PodUID: podUID, Container: container, ContainerID: e.Name(), CgroupDir: dir}, nil
		}
	}
	return Target{}, fmt.Errorf("agent: container %q of pod %s not found under %v", container, podUID, roots)
}

// findCgroupDir locates the cgroup directory of a container id under the
// kubepods hierarchy (cri-containerd-<id>.scope with the systemd driver,
// <id> with the cgroupfs driver).
func findCgroupDir(root, id string) (string, error) {
	var found string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable subtree: keep looking
		}
		if !d.IsDir() {
			return nil
		}
		if filepath.Dir(path) == root && !strings.HasPrefix(d.Name(), "kubepods") {
			return fs.SkipDir
		}
		if strings.Contains(d.Name(), id) {
			found = path
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", errors.New("agent: cgroup directory for container " + id + " not found under " + root)
	}
	return found, nil
}

// ResolverFunc adapts a function to TargetResolver (tests).
type ResolverFunc func(podUID, container string) (Target, error)

// Resolve implements TargetResolver.
func (f ResolverFunc) Resolve(podUID, container string) (Target, error) { return f(podUID, container) }

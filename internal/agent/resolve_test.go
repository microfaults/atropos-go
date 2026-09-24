package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	uidA = "2c4c1e5b-7a5e-4c05-9d3b-2c2f0a1c6c1e"
	uidB = "9f8e7d6c-5b4a-4c3d-8e2f-1a0b9c8d7e6f"
)

func writeSpec(t *testing.T, taskRoot, id string, ann map[string]string) {
	t.Helper()
	dir := filepath.Join(taskRoot, "k8s.io", id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString(`{"ociVersion":"1.1.0","annotations":{`)
	first := true
	for k, v := range ann {
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(`"` + k + `":"` + v + `"`)
	}
	b.WriteString(`},"linux":{"cgroupsPath":"kubepods-burstable-pod.slice:cri-containerd:` + id + `"}}`)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fakeNode lays out containerd task specs and a cgroup v2 tree for two pods.
func fakeNode(t *testing.T) (taskRoot, cgroupRoot string, dirA string) {
	t.Helper()
	base := t.TempDir()
	taskRoot = filepath.Join(base, "task")
	cgroupRoot = filepath.Join(base, "cgroup")
	writeSpec(t, taskRoot, "aaa111", map[string]string{annSandboxUID: uidA, annContainerName: "server", annContainerType: "container"})
	writeSpec(t, taskRoot, "aaa222", map[string]string{annSandboxUID: uidA, annContainerName: "atroposd", annContainerType: "container"})
	writeSpec(t, taskRoot, "aaa000", map[string]string{annSandboxUID: uidA, annContainerType: "sandbox"})
	writeSpec(t, taskRoot, "bbb111", map[string]string{annSandboxUID: uidB, annContainerName: "server", annContainerType: "container"})
	podA := filepath.Join(cgroupRoot, "kubepods.slice", "kubepods-burstable.slice", "kubepods-burstable-pod"+strings.ReplaceAll(uidA, "-", "_")+".slice")
	podB := filepath.Join(cgroupRoot, "kubepods.slice", "kubepods-besteffort.slice", "kubepods-besteffort-pod"+strings.ReplaceAll(uidB, "-", "_")+".slice")
	for _, d := range []string{
		filepath.Join(podA, "cri-containerd-aaa111.scope"), filepath.Join(podA, "cri-containerd-aaa222.scope"), filepath.Join(podA, "cri-containerd-aaa000.scope"),
		filepath.Join(podB, "cri-containerd-bbb111.scope"),
		filepath.Join(cgroupRoot, "system.slice", "cri-containerd-aaa111.scope"), // decoy outside kubepods: must be skipped
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dirA = filepath.Join(podA, "cri-containerd-aaa111.scope")
	for _, f := range []struct{ name, content string }{{"cpu.max", "max 100000\n"}, {"memory.max", "1073741824\n"}, {"memory.high", "max\n"}, {"cgroup.procs", ""}} {
		if err := os.WriteFile(filepath.Join(dirA, f.name), []byte(f.content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return taskRoot, cgroupRoot, dirA
}

func TestResolver_FindsContainerByAnnotationsAndCgroupById(t *testing.T) {
	taskRoot, cgroupRoot, dirA := fakeNode(t)
	r := &Resolver{TaskRoots: []string{"/nonexistent", taskRoot}, CgroupRoot: cgroupRoot}

	tg, err := r.Resolve(uidA, "server")
	if err != nil {
		t.Fatal(err)
	}
	if tg.ContainerID != "aaa111" || tg.CgroupDir != dirA {
		t.Fatalf("target = %+v, want aaa111 at %s", tg, dirA)
	}
	if tg, err := r.Resolve(uidB, "server"); err != nil || tg.ContainerID != "bbb111" {
		t.Fatalf("pod B: %+v %v", tg, err)
	}
	if _, err := r.Resolve(uidA, "nope"); err == nil {
		t.Fatal("unknown container must not resolve")
	}
	if _, err := r.Resolve(uidA, ""); err == nil {
		t.Fatal("the sandbox (pause) container must never resolve")
	}
}

func TestPodUIDFromCgroup(t *testing.T) {
	systemd := "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod2c4c1e5b_7a5e_4c05_9d3b_2c2f0a1c6c1e.slice/cri-containerd-aaa222.scope\n"
	cgroupfs := "0::/kubepods/burstable/pod2c4c1e5b-7a5e-4c05-9d3b-2c2f0a1c6c1e/aaa222\n"
	for _, c := range []string{systemd, cgroupfs} {
		got, err := PodUIDFromCgroup(c)
		if err != nil || got != uidA {
			t.Fatalf("%q → %q, %v", c, got, err)
		}
	}
	if _, err := PodUIDFromCgroup("0::/system.slice/sshd.service\n"); err == nil {
		t.Fatal("a non-pod process must not yield a pod uid")
	}
}

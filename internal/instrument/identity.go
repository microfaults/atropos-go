package instrument

import "os"

// ResolveInstanceID picks the identity manteion correlates an SDK instance
// under. It MUST be identical across the register call, the cache-push
// envelopes/drain reports, and the fidelity endpoint, so hosts resolve it
// exactly once and thread it everywhere. Precedence: explicit >
// MANTEION_INSTANCE_ID > hostname (the pod name in k8s) > service name.
func ResolveInstanceID(explicit, service string) string {
	if explicit != "" {
		return explicit
	}
	if id := os.Getenv("MANTEION_INSTANCE_ID"); id != "" {
		return id
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return service
}

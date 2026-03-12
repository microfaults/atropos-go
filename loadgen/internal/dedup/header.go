package dedup

import "net/http"

// HeaderMutator sets a header to a unique value on each request.
// Common use: X-Idempotency-Key, X-Request-ID.
type HeaderMutator struct {
	HeaderName string
}

func (h *HeaderMutator) Transform(req *http.Request) *http.Request {
	clone := req.Clone(req.Context())
	clone.Header.Set(h.HeaderName, generateUniqueID())
	return clone
}

var _ DedupBypass = (*HeaderMutator)(nil)

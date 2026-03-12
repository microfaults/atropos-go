package dedup

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// DedupBypass mutates an HTTP request to avoid idempotency deduplication.
// Implementations must be safe for concurrent use and must not modify
// the original request (use req.Clone).
type DedupBypass interface {
	Transform(req *http.Request) *http.Request
}

// generateUniqueID produces a 32-char hex string from 16 random bytes.
func generateUniqueID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

package cachebox

import (
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
)

// KeyFunc derives a cache key from an HTTP request. Must be deterministic
// and collision-resistant for the traffic patterns of the cached service.
// The returned string is used both for the local store lookup and as the
// X-Atropos-Cache-Key response header value.
//
// Implementations must be safe to call from both the synchronous replay
// path (where body is typically nil) and the asynchronous recorder drain
// goroutine.
type KeyFunc func(r *http.Request, body []byte) string

// KeyStrategy names a built-in key derivation approach that the coordinator
// can look up by name. External callers can also supply a custom KeyFunc
// via cachebox.Config.KeyFunc.
type KeyStrategy string

const (
	// KeyStrategyExact keys on method + path + normalized query string.
	// Zero cost on the hot path -- no body read, no hashing.
	KeyStrategyExact KeyStrategy = "exact"

	// KeyStrategyExactWithHost is like Exact but includes the Host header.
	// Useful when multiple upstreams are reached through a single transport.
	KeyStrategyExactWithHost KeyStrategy = "exact_with_host"

	// KeyStrategyExactWithBody is like ExactWithHost plus a FNV-1a hash of
	// the request body. Required for endpoints where the body determines
	// the response (e.g. POST /search with a JSON query). Forces the hot
	// path to buffer the request body.
	KeyStrategyExactWithBody KeyStrategy = "exact_with_body"
)

// NeedsBody reports whether the strategy requires the request body for
// key derivation. The coordinator uses this to decide whether to buffer
// the body on the hot path.
func (k KeyStrategy) NeedsBody() bool {
	return k == KeyStrategyExactWithBody
}

// KeyFuncFor returns the built-in KeyFunc for the named strategy. Unknown
// strategies fall back to KeyStrategyExact.
func KeyFuncFor(s KeyStrategy) KeyFunc {
	switch s {
	case KeyStrategyExact:
		return exactKey
	case KeyStrategyExactWithHost:
		return exactWithHostKey
	case KeyStrategyExactWithBody:
		return exactWithBodyKey
	default:
		return exactKey
	}
}

func exactKey(r *http.Request, _ []byte) string {
	var b strings.Builder
	b.Grow(len(r.Method) + len(r.URL.Path) + len(r.URL.RawQuery) + len(KeyStrategyExact) + 4)
	b.WriteString(string(KeyStrategyExact))
	b.WriteByte(':')
	b.WriteString(r.Method)
	b.WriteByte('|')
	b.WriteString(r.URL.Path)
	writeQueryPart(&b, r.URL.RawQuery)
	return b.String()
}

func exactWithHostKey(r *http.Request, _ []byte) string {
	var b strings.Builder
	b.Grow(len(r.Method) + len(r.Host) + len(r.URL.Path) + len(r.URL.RawQuery) + len(KeyStrategyExactWithHost) + 6)
	b.WriteString(string(KeyStrategyExactWithHost))
	b.WriteByte(':')
	b.WriteString(r.Method)
	b.WriteByte('|')
	b.WriteString(r.Host)
	b.WriteByte('|')
	b.WriteString(r.URL.Path)
	writeQueryPart(&b, r.URL.RawQuery)
	return b.String()
}

func exactWithBodyKey(r *http.Request, body []byte) string {
	h := fnv.New64a()
	if len(body) > 0 {
		h.Write(body)
	}
	hash := strconv.FormatUint(h.Sum64(), 16)

	var b strings.Builder
	b.Grow(len(r.Method) + len(r.Host) + len(r.URL.Path) + len(r.URL.RawQuery) + len(hash) + len(KeyStrategyExactWithBody) + 8)
	b.WriteString(string(KeyStrategyExactWithBody))
	b.WriteByte(':')
	b.WriteString(r.Method)
	b.WriteByte('|')
	b.WriteString(r.Host)
	b.WriteByte('|')
	b.WriteString(r.URL.Path)
	writeQueryPart(&b, r.URL.RawQuery)
	b.WriteByte('|')
	b.WriteString(hash)
	return b.String()
}

// writeQueryPart appends "?normalized" to b if raw is non-empty.
func writeQueryPart(b *strings.Builder, raw string) {
	if raw == "" {
		return
	}
	b.WriteByte('?')
	b.WriteString(normalizeQuery(raw))
}

// normalizeQuery sorts query params alphabetically so "a=1&b=2" and
// "b=2&a=1" produce the same key. For single-param queries it returns
// the input unchanged.
func normalizeQuery(raw string) string {
	if !strings.Contains(raw, "&") {
		return raw
	}
	parts := strings.Split(raw, "&")
	sortStrings(parts)
	return strings.Join(parts, "&")
}

// sortStrings is a tiny insertion sort over a string slice. We use it
// instead of "sort" to keep this package's dependency footprint minimal
// and because query param lists are short (typically <20 entries).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

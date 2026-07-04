package cachebox

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

	// KeyStrategyCanonicalV2 is the length-prefixed SHA-256 framing keyer
	// (method + host + path + canonical query + header fingerprint + body
	// hash; wire spec §W7). It is the authoritative default for new rules;
	// the keying itself is implemented in ATRO-3.
	KeyStrategyCanonicalV2 KeyStrategy = "canonical_v2"
)

// NeedsBody reports whether the strategy requires the request body for
// key derivation. The coordinator uses this to decide whether to buffer
// the body on the hot path.
func (k KeyStrategy) NeedsBody() bool {
	return k == KeyStrategyExactWithBody || k == KeyStrategyCanonicalV2
}

// KeyFuncFor returns the built-in KeyFunc for the named strategy. Unknown
// strategies fall back to KeyStrategyExact. This is the construction-time
// path (CacheBox.Config.KeyStrategy) -- it has no per-rule key_headers to
// work with, so its canonical_v2 keyer uses the default header allowlist
// only. Derive is the per-request path used once a rule's CacheBoxContext
// (and its key_headers) is available.
func KeyFuncFor(s KeyStrategy) KeyFunc {
	switch s {
	case KeyStrategyExact:
		return exactKey
	case KeyStrategyExactWithHost:
		return exactWithHostKey
	case KeyStrategyExactWithBody:
		return exactWithBodyKey
	case KeyStrategyCanonicalV2:
		return func(r *http.Request, body []byte) string {
			return canonicalV2Key(r, body, nil)
		}
	default:
		return exactKey
	}
}

// Derive resolves a KeyFunc for strategy -- honoring keyHeaders for
// canonical_v2 -- and applies it to the request. This is the per-request
// path used once a rule's CacheBoxContext is available (design doc Q3):
// the matched rule's context is authoritative end-to-end, so its
// KeyStrategy/KeyHeaders take precedence over the CacheBox's
// construction-time default (KeyFuncFor), which remains only as the
// fallback for when no context is present.
func Derive(strategy KeyStrategy, keyHeaders []string, r *http.Request, body []byte) string {
	if strategy == KeyStrategyCanonicalV2 {
		return canonicalV2Key(r, body, keyHeaders)
	}
	return KeyFuncFor(strategy)(r, body)
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

// defaultKeyHeaderAllowlist is the base set of headers canonical_v2 folds
// into the key fingerprint, before any per-rule key_headers additions
// (design doc Q3). Everything else (Date, Authorization, Cookie,
// User-Agent, traceparent, X-Request-Id, Content-Length, ...) is excluded
// by construction: it is simply never in this set.
var defaultKeyHeaderAllowlist = []string{"accept", "content-type", "accept-encoding", "accept-language"}

// canonicalV2Key implements wire spec §W7: key = "v2:" + hex(SHA-256(frame)),
// frame = length-prefixed concatenation of (in order) "v2", uppercased
// method, lowercased URL host, raw URL path, canonical query, header
// fingerprint (§Q3, extra headers from keyHeaders), and SHA-256(body) (an
// empty component when body is empty).
func canonicalV2Key(r *http.Request, body []byte, keyHeaders []string) string {
	var bodyComponent []byte
	if len(body) > 0 {
		sum := sha256.Sum256(body)
		bodyComponent = sum[:]
	}

	frame := canonicalV2Frame(
		[]byte("v2"),
		[]byte(strings.ToUpper(r.Method)),
		[]byte(strings.ToLower(r.URL.Host)),
		[]byte(r.URL.Path),
		[]byte(normalizeQuery(r.URL.RawQuery)),
		[]byte(headerFingerprint(r.Header, keyHeaders)),
		bodyComponent,
	)
	digest := sha256.Sum256(frame)
	return "v2:" + hex.EncodeToString(digest[:])
}

// canonicalV2Frame builds the length-prefixed frame from ordered
// components: uvarint(len(c)) || c for each c, concatenated. Length-
// prefixing (rather than a joining delimiter) is what makes ("ab","c") and
// ("a","bc") produce different frames -- a delimiter can't distinguish a
// component containing it from a component boundary.
func canonicalV2Frame(components ...[]byte) []byte {
	var buf []byte
	for _, c := range components {
		buf = binary.AppendUvarint(buf, uint64(len(c)))
		buf = append(buf, c...)
	}
	return buf
}

// headerFingerprint builds the §Q3 header-fingerprint component: for each
// *present* header in (defaultKeyHeaderAllowlist ∪ extra), in sorted
// lowercase-name order, the line "name:" + values joined by ",", lines
// joined by "\n". Absent headers contribute nothing -- absence is not the
// same as an empty value, so there is no placeholder line for them.
func headerFingerprint(h http.Header, extra []string) string {
	seen := make(map[string]bool, len(defaultKeyHeaderAllowlist)+len(extra))
	names := make([]string, 0, len(defaultKeyHeaderAllowlist)+len(extra))
	for _, n := range defaultKeyHeaderAllowlist {
		n = strings.ToLower(n)
		if !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	for _, n := range extra {
		n = strings.ToLower(n)
		if !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	sortStrings(names)

	var lines []string
	for _, name := range names {
		vals := h.Values(name)
		if len(vals) == 0 {
			continue
		}
		lines = append(lines, name+":"+strings.Join(vals, ","))
	}
	return strings.Join(lines, "\n")
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

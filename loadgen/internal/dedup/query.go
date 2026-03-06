package dedup

import "net/http"

// QueryParamMutator appends or replaces a query parameter with a unique value.
// Common use: ?nonce=xxx, ?request_id=xxx.
type QueryParamMutator struct {
	ParamName string
}

func (q *QueryParamMutator) Transform(req *http.Request) *http.Request {
	clone := req.Clone(req.Context())
	query := clone.URL.Query()
	query.Set(q.ParamName, generateUniqueID())
	clone.URL.RawQuery = query.Encode()
	return clone
}

var _ DedupBypass = (*QueryParamMutator)(nil)

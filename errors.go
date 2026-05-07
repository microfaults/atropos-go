package atropos

import (
	"fmt"
	"net/http"
)

// ErrorResponse is the JSON envelope returned by admin handler errors.
type ErrorResponse struct {
	Error string `json:"error" example:"invalid json"`
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

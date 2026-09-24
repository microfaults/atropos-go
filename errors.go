package atropos

import (
	"fmt"
	"net/http"
)

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

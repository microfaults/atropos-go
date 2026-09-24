package wire

// ErrorResponse is the JSON envelope returned by admin handler errors.
type ErrorResponse struct {
	Error string `json:"error" example:"invalid json"`
}

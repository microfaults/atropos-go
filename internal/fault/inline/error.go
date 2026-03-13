package inline

import (
	"context"
	"fmt"

	fault "github.com/microfaults/atropos-go/internal/fault"
)

// Error is an inline fault that completes immediately with a configured
// error. Used to simulate dependency failures at injection points.
//
// The error is delivered via Result.Detail (as ErrorDetail), not Result.Err,
// because the fault itself executed successfully — the error is the
// intended effect, not a failure in fault execution.
type Error struct {
	fault.FaultConfig

	// StatusCode is the HTTP status code to simulate (e.g., 503).
	StatusCode int

	// Message is the error message.
	Message string
}

// ErrorDetail is the Detail payload for an Error fault result.
type ErrorDetail struct {
	StatusCode int
	Message    string
}

func (e *Error) Validate() error {
	if e.StatusCode < 100 || e.StatusCode > 599 {
		return fmt.Errorf("inline/error: status_code must be 100-599, got %d", e.StatusCode)
	}
	return nil
}

func (e *Error) Start(ctx context.Context) (*fault.Handle, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}

	_, cancel := context.WithCancel(ctx)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()
		handle.Send(fault.Result{
			ActualDuration: 0,
			Detail: ErrorDetail{
				StatusCode: e.StatusCode,
				Message:    e.Message,
			},
		})
	}()

	return handle, nil
}

// Err returns the error detail as a standard error for convenience.
func (d ErrorDetail) Err() error {
	return fmt.Errorf("injected error: %d %s", d.StatusCode, d.Message)
}

// IsErrorResult checks if a fault.Result contains an ErrorDetail
// and returns it. Returns nil if not an error fault result.
func IsErrorResult(r fault.Result) *ErrorDetail {
	if d, ok := r.Detail.(ErrorDetail); ok {
		return &d
	}
	return nil
}

var _ fault.Fault = (*Error)(nil)

// StatusText returns the message, defaulting to a generic message
// if empty.
func (d ErrorDetail) StatusText() string {
	if d.Message != "" {
		return d.Message
	}
	return fmt.Sprintf("injected fault: HTTP %d", d.StatusCode)
}

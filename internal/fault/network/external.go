package network

import "context"

// ExternalProxy is a hook for delegating network fault injection to an
// external proxy such as Toxiproxy. When set on Config, the built-in
// TCP proxy is bypassed and all lifecycle calls go through this interface.
//
// A Toxiproxy implementation would call its REST API to create/configure
// proxies and add/remove toxics.
type ExternalProxy interface {
	// Apply configures the external proxy with the given fault parameters.
	// listen and upstream define the proxy endpoints. toxics are the
	// effects to apply (the implementation maps them to the external
	// proxy's native toxic format).
	Apply(ctx context.Context, listen, upstream string, toxics []ToxicLink) error

	// Remove tears down the external proxy configuration.
	Remove(ctx context.Context) error
}

// Package wire declares the JSON wire shapes shared by the atropos SDK
// hosts (the in-process Serve embed and the atroposd sidecar) and the
// manteion control plane. Every type here is decoded from / encoded to one
// of the SDK's HTTP endpoints or embedded in the manteion sync protocol.
// The root atropos package re-exports each type under the same name as a
// type alias, so the shapes manteion imports are unaffected by their move.
//
// Field sets and json tags are pinned by the cross-repo wire tests
// (wire_fidelity_test.go in the root package, wire_compat_test.go in
// internal/instrument) and must not drift.
package wire

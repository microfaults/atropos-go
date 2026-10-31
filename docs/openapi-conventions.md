# OpenAPI Annotation Conventions

All admin handlers in this repo are annotated with [swaggo/swag](https://github.com/swaggo/swag) v2 to generate OpenAPI 3.1. Host services that mount these handlers can publish a combined spec or reference this SDK spec directly.

## Toolchain

- `swag` v2.0.0-rc5 is pinned. Install with `go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5`.
- The generated spec is committed at `docs/swagger.{yaml,json}` and validated in CI with `make openapi-check`.
- `scripts/strip-empty-externaldocs.py` removes empty `externalDocs` stubs emitted by current swag v2 builds.
- swag v2 cannot parse the Go 1.27 standard library (`make openapi` fails in `math/rand/v2` there), so regenerate with the module's Go 1.25 toolchain: `GOTOOLCHAIN=go1.25.5 make openapi`.
- swag emits only the schemas an annotated operation reaches, so a wire type no route carries is absent from the spec however it is annotated.

## Required annotations

- `@Summary` for each documented operation.
- `@Description` when behavior needs more than the summary.
- `@Tags`, usually `admin` or `health`. An operation that documents an endpoint manteion serves and the SDK calls (not one `Serve` mounts) uses `manteion-protocol`, and its `@Description` says so.
- `@Accept json` only for routes with JSON request bodies.
- `@Produce json` for JSON responses.
- `@Success` and `@Failure` for documented status codes.
- `@Router` with the recommended mount path and method.

## Regenerating

Run `make openapi` from the repo root and commit `docs/swagger.yaml` and `docs/swagger.json` together with handler changes.

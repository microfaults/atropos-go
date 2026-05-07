# OpenAPI spec generation via swaggo/swag v2.
#
# Usage:
#   make openapi        Regenerate docs/swagger.{yaml,json} from handler annotations.
#   make openapi-check  Regenerate then assert the result is unchanged (CI gate).
#
# Requirements:
#   - swag v2 binary on PATH; install with:
#       go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5
#   - python3 with pyyaml for scripts/strip-empty-externaldocs.py

.PHONY: openapi openapi-check test build

GOCACHE ?= $(CURDIR)/.gocache
export GOCACHE

openapi:
	swag init \
		--v3.1 \
		--generalInfo atropos.go \
		--dir . \
		--output docs \
		--outputTypes yaml,json \
		--parseDependency \
		--parseInternal \
		--exclude internal/cachebox/testdata,internal/evaluator/testdata
	@python3 scripts/strip-empty-externaldocs.py docs/swagger.yaml docs/swagger.json

openapi-check: openapi
	@git diff --exit-code -- docs/swagger.yaml docs/swagger.json || \
		(echo "ERROR: swagger spec is stale. Run 'make openapi' and commit." && exit 1)

test:
	go test ./...

build:
	go build ./...

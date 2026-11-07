# atropos-agent image. Build from the repository root:
#   docker build -f deploy/agent/Dockerfile -t atropos-agent:dev .
# The build stage runs on the build host's platform and cross-compiles for
# the target (emulating the Go compiler under another arch can segfault).
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /atropos-agent ./cmd/atropos-agent

FROM alpine:3.22
# tc and ip (iproute2) and nsenter (util-linux) execute netem faults; the
# build fails here if either binary is missing.
RUN apk add --no-cache iproute2 util-linux && tc -V && nsenter -V
COPY --from=build /atropos-agent /usr/local/bin/atropos-agent
# Runs as root: it moves processes between cgroups and writes cgroup limits.
ENTRYPOINT ["/usr/local/bin/atropos-agent", "serve"]

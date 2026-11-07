# atroposd sidecar image. Build from the repository root:
#   docker build -f deploy/sidecar/Dockerfile -t atroposd:dev .
# The build stage runs on the build host's platform and cross-compiles for
# the target (emulating the Go compiler under another arch can segfault).
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /atroposd ./cmd/atroposd

FROM alpine:3.22
# uid 1337 is what init.sh excludes from the REDIRECT: the sidecar's own
# connections (to upstreams, manteion, the collector) must not loop back.
RUN adduser -D -u 1337 atropos && mkdir -p /var/run/atropos && chown atropos /var/run/atropos
COPY --from=build /atroposd /usr/local/bin/atroposd
USER 1337
EXPOSE 16001 16006 16020
ENTRYPOINT ["/usr/local/bin/atroposd"]

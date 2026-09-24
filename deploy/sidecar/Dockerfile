# atroposd sidecar image. Build from the repository root:
#   docker build -f deploy/sidecar/Dockerfile -t atroposd:dev .
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /atroposd ./cmd/atroposd

FROM alpine:3.22
# uid 1337 is what init.sh excludes from the REDIRECT: the sidecar's own
# connections (to upstreams, manteion, the collector) must not loop back.
RUN adduser -D -u 1337 atropos && mkdir -p /var/run/atropos && chown atropos /var/run/atropos
COPY --from=build /atroposd /usr/local/bin/atroposd
USER 1337
EXPOSE 16001 16006 16020
ENTRYPOINT ["/usr/local/bin/atroposd"]

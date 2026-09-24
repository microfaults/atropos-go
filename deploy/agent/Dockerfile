# atropos-agent image. Build from the repository root:
#   docker build -f deploy/agent/Dockerfile -t atropos-agent:dev .
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /atropos-agent ./cmd/atropos-agent

FROM alpine:3.22
COPY --from=build /atropos-agent /usr/local/bin/atropos-agent
# Runs as root: it moves processes between cgroups and writes cgroup limits.
ENTRYPOINT ["/usr/local/bin/atropos-agent", "serve"]

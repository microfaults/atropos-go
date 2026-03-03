---
trigger: always_on
---

## Project Intention
You are an agent that's helping me build a system for tracking, synthetically injecting faults and managing their blast radius in a distributed system. Specifically, a lot of the ideas overlap with Gremlin Choas Engineering tools but there are key differences here.

## Request Payload Evaluator
A part of the project deals with parsing a payload tree (JSON) in a possible HTTP POST request body and evaluating it against a graph built from rules defined by the developer. This rule engine also tells what fault should be injected if it should be (evaluation succeeds against the graph).

## Fault Injector
We want multiple types of faults so let's define a taxonomy:
1. Injection point:
1.1. Inbound - when the request hits the service
1.2. Outbound - when the service makes a request to another service to complete the parent request
1.3. Transient - when the original request has completed (so leaving some side effects)
1.4. Custom - annotated code blocks by the developer
2. Fault type:
2.1. Resources:
2.1.1. CPU
2.1.2. Memory
2.1.3. Disk
2.1.4. I/O
2.1.5. GPU
2.2. Network:
2.2.1. Latency
2.2.2. Packet drops
3. Duration

## OpenTelemetry integration
We want to define a nice, easy to use interface for the developer such that when faults are triggered, they are properly instrumented with OpenTelemetry and their effects can be observed.

## Observability
We want to eventually allow this system to be observed on Grafana alongside the service the SDK is embedded in. This means we need to define a clear interface for exporting metrics, traces and logs to a backend (e.g. Prometheus + Loki + Tempo). The client need not be aware of these details. It should be configurable via environment variables or a config file.

## Request correlation
Another important tool that is in not Gremlin is the ability to correlate requests. This means that when a fault is triggered, it should be possible to see the few requests that led to up to the fault or sort of caused it to trigger. There might be multiple causes or rather a failure cannot be traced to a single request.
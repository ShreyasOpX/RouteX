# RouteX Ride API

## Overview

The API is the entry point into the RouteX event pipeline.

## Why this exists

It defines the HTTP boundary before Kafka takes ownership of event delivery.

## Problem it solves

It documents valid input and the difference between one publish and bulk send-future waiting.

## RouteX implementation

`RideRequestController` exposes `POST /rides/requests` and `POST /rides/bulk/{count}` under `/rides`.

## Code walkthrough

Read `dispatch/RideRequestController.java`, `RideRequest.java`, and `response/BenchmarkResponse.java`.

## Execution flow

```mermaid
flowchart LR
 HTTP --> Validate[RideRequest validation] --> Event[RideRequestedEvent] --> Kafka
```

## Kafka internals

The one-request endpoint initiates a Kafka send; the bulk endpoint waits for all collected send futures before responding.

## Spring Boot internals

Jakarta validation enforces `@NotBlank` fields; Spring MVC serializes records/responses.

## Observe this in RouteX

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/rides/bulk/10
```

The response includes `recordsPublished`, `durationMs`, and `throughputPerSecond` from the controller calculation.

## Production implementation

| RouteX | Production |
| --- | --- |
| Generated ride ID | Domain creation and persistence strategy |
| Learning benchmark response | Authenticated, rate-limited API with SLA semantics |

## Trade-offs

Asynchronous intake improves decoupling but callers need an explicit status model for completed workflows.

## Common mistakes

- Reading `202` as downstream completion.
- Using the bulk endpoint as a production load test.

## Best practices

Validate inputs, retain correlation IDs, and document asynchronous semantics.

## Interview questions

**Why does the bulk API wait?** It intentionally waits for send futures to calculate its local result.

**Follow-up:** Does that wait for consumers? No.

**Enterprise discussion:** API contracts should make command acceptance and business completion distinct.

## Quick revision

`/requests` publishes one event; `/bulk/{count}` waits for producer futures, not consumer completion.


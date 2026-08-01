# RouteX Kafka FAQ

## Overview

This FAQ answers common questions using RouteX source as evidence.

## Why this exists

Readers often need short answers after studying the longer guides.

## Problem it solves

It prevents unsupported assumptions about the demo.

## RouteX implementation

The answers refer to current topics, listeners, configuration, and missing capabilities.

## Code walkthrough

Use named classes in each answer to verify the claim.

## Execution flow

```mermaid
flowchart LR
 Question --> Source[RouteX source] --> Answer --> Guide[Detailed guide]
```

## Kafka internals

Kafka stores partition records and tracks consumption by group; the FAQ does not replace detailed internals pages.

## Spring Boot internals

Boot configures Spring Kafka infrastructure; `@KafkaListener` methods run in listener containers.

## Observe this in RouteX

Use the normal request for producer/consumer flow and `failure-test` for retries/DLT.

## Production implementation

| RouteX | Production |
| --- | --- |
| Learning demo | Requirement-driven design and operations |
| No tests/security/persistence | These capabilities require implementation before production use |

## Trade-offs

Kafka improves decoupling and replayability while adding distributed-system operational responsibilities.

## Common mistakes

- **Does RouteX send email/SMS?** No; notification is console output only.
- **Does RouteX persist drivers or rides?** No; drivers are an in-memory list and no database is configured.
- **Does `failure-test` publish an assignment?** No; it throws before assignment publishing.
- **Does a DLT replay automatically?** No; RouteX only monitors recovered records.

## Best practices

Verify behavior in code and Kafka UI before extending a learning assumption into production guidance.

## Interview questions

**Is RouteX a production architecture?** It is a production-inspired learning application; several production capabilities are not implemented.

**Follow-up:** Which capabilities? See the production guide for topology, security, schema, persistence, testing, and operations gaps.

**Enterprise discussion:** Good architecture documentation names boundaries and omissions precisely.

## Quick revision

RouteX demonstrates event flow, partitions, groups, ack, retry, and DLT; it does not implement persistence, security, or tests.

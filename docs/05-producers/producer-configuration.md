# RouteX Producer Configuration

## Overview

`application.yaml` configures serializers, `acks`, retries, batch size, zstd compression, idempotence, max in-flight requests, and linger.

## Why this exists

These settings turn the controller's send call into a latency, throughput, and durability decision.

## Problem it solves

It explains configuration in the context of RouteX sends instead of listing properties.

## RouteX implementation

The producer section is the source of truth; `transaction-id-prefix` is configured, while no active producer code calls `executeInTransaction`.

## Code walkthrough

Read `src/main/resources/application.yaml` with both producer call sites.

## Execution flow

```mermaid
flowchart LR
 Event --> Serialize --> Buffer -->|linger/batch| Compress -->|acks| Broker
```

## Kafka internals

`acks=all` waits for all in-sync replicas; idempotence uses producer identity and sequence numbers to avoid duplicate appends on retry.

## Spring Boot internals

Boot binds standard producer fields and passes entries under `properties` to the Kafka client.

## Observe this in RouteX

The configured DEBUG/TRACE Kafka client loggers reveal producer and network lifecycle. Compare `PRODUCER ACK` metadata after sends.

## Production implementation

| RouteX | Production |
| --- | --- |
| Local broker | Replication and ISR policy matched to durability targets |
| Fixed tuning | Load-tested, workload-specific tuning |
| Configured transaction prefix | Transaction use only when application code defines transaction boundaries |

## Trade-offs

Linger and batching increase throughput but add waiting; stronger acknowledgements improve durability but can increase latency.

## Common mistakes

- Setting values without measuring workload effects.
- Calling configured transaction capability an implemented exactly-once workflow.

## Best practices

Explain settings alongside failure and latency requirements; verify effective runtime properties.

## Interview questions

**Why `acks=all`?** It requests acknowledgement from all in-sync replicas.

**Follow-up:** What does idempotence prevent? Duplicate broker appends caused by producer retries.

**Enterprise discussion:** Tune producer settings with broker replication, SLA, and payload characteristics.

## Quick revision

RouteX producer config balances durable acknowledgements with batching and compression; transactions are not executed by active code.


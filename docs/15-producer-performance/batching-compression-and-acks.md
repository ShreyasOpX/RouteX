# Producer Performance Settings

## Overview

RouteX configures `batch-size: 32768`, `linger.ms: 500`, zstd compression, and `acks: all`.

## Why this exists

The request producer demonstrates that throughput, latency, and durability are connected choices.

## Problem it solves

It explains why `bulkPublish` measures send-future completion rather than simply looping over method calls.

## RouteX implementation

`RideRequestController.bulkPublish` collects send futures and waits with `CompletableFuture.allOf(...).join()`.

## Code walkthrough

Read `bulkPublish` and the producer section of `application.yaml`.

## Execution flow

```mermaid
flowchart LR
 Records --> Buffer -->|batch size or linger| Z[zstd compression] --> Broker --> Futures
```

## Kafka internals

Kafka batches records per partition. Linger can wait for more records; compression reduces network/storage bytes; acknowledgements determine delivery confirmation.

## Spring Boot internals

Boot maps direct producer properties and passes low-level entries in `producer.properties` to the Kafka client.

## Observe this in RouteX

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/rides/bulk/100
```

Compare the returned duration with producer ACK output; this endpoint is a learning benchmark, not a load-testing framework.

## Production implementation

| RouteX | Production |
| --- | --- |
| Static 500ms linger | Latency budget tested per workload |
| One local broker | Network and replication-aware durability measurement |
| Simple bulk endpoint | Dedicated performance test and telemetry |

## Trade-offs

Larger batches and linger improve throughput/compression efficiency but can delay individual records.

## Common mistakes

- Treating producer call time as broker acknowledgement time.
- Choosing linger without an end-user latency budget.

## Best practices

Measure end-to-end latency, payload size, partition distribution, and broker load before tuning.

## Interview questions

**What triggers batch send?** Batch fullness or linger expiration, among other client conditions.

**Follow-up:** Why compress? To reduce bytes at CPU cost.

**Enterprise discussion:** Tune by workload, not copied defaults.

## Quick revision

Batching trades latency for throughput; zstd trades CPU for fewer bytes; `acks=all` favors durability.


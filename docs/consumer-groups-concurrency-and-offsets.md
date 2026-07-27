# Consumer Groups, Concurrency, and Offsets

## Current RouteX Configuration

| Topic | Listener | Group ID | Concurrency |
| --- | --- | --- | --- |
| `ride-requested` | `DriverMatchingConsumer` | `driver-matching-group` | No explicit setting; Spring Kafka default is 1 |
| `driver-assigned` | `DriverAssignmentNotificationConsumer` | `notification-group` | No explicit setting; Spring Kafka default is 1 |
| `ride-requested-dlt` | `DeadLetterConsumer` | `routex-dlt-monitor` | No explicit setting; Spring Kafka default is 1 |

`routex-ride-request-logger` is the YAML default group ID, but all active listeners set their own `groupId`, so it is not their effective group.

Consumers in one group cooperate: Kafka assigns a partition to at most one active consumer within that group. A consumer can own multiple partitions. Separate groups can independently read the same topic because their progress is separate.

```text
3 partitions + 3 consumers: P0 -> C1, P1 -> C2, P2 -> C3
3 partitions + 5 consumers: three active consumers, two idle
5 partitions + 2 consumers: each consumer owns multiple partitions

maximum useful active consumers = min(partitions, consumers)
```

Do not confuse topic partitions with listener concurrency:

```text
TopicBuilder.partitions(3)       = three Kafka partitions
@KafkaListener(concurrency="3") = three Spring consumer instances
```

RouteX configures the first and not the second. One listener instance can therefore be assigned all three partitions today.

## Offsets and Manual Acknowledgement

Offsets identify a record position within a partition:

```text
P0 offset 5
P1 offset 5
P2 offset 5
```

These are three positions, not one globally unique offset. A group's committed offset is its saved progress for a particular topic partition, distinct from a record's offset. `auto-offset-reset: earliest` applies only where a group has no committed position.

`spring.kafka.listener.ack-mode: manual` makes successful listener paths explicitly call `Acknowledgment.acknowledge()`. The matching listener intentionally does not acknowledge `failure-test` because it throws first; the error handler then controls retry/recovery.

## Practical Verification

### Prerequisites

Start Docker Kafka and RouteX as shown in the [README](../README.md#run-locally).

### Trigger

```powershell
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8080/rides/requests" `
    -ContentType "application/json" `
    -Body '{"passengerId":"passenger-101","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
```

### Expected Output

```text
MATCHING | ride=<generated-uuid> | partition=<0|1|2> | offset=<dynamic-offset>
```

### What This Demonstrates

The partition/offset pair identifies exactly where the matching group consumed this record. The current logs do not expose consumer thread or client ID, so they cannot prove concurrent consumer-instance assignment. Use Kafka UI's consumer-group view to inspect assignments; do not infer concurrency from the current console output.

## Key Takeaways

- Groups distribute partitions; a partition is exclusive only within one group.
- More consumers than partitions creates idle consumers for that topic/group.
- Offsets are partition-specific positions, and manual acknowledgement makes the success boundary explicit.

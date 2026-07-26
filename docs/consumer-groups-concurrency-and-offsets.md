# Consumer Groups, Concurrency, and Offsets

## Current Groups

| Topic | Listener | Effective group ID | Configured concurrency |
| --- | --- | --- | --- |
| `ride-requested` | `DriverMatchingConsumer.handleRideRequested` | `driver-matching-group` | None; Spring Kafka default is 1. |
| `driver-assigned` | `DriverAssignmentNotificationConsumer.handleDriverAssigned` | `notification-group` | None; Spring Kafka default is 1. |

`application.yaml` also contains `spring.kafka.consumer.group-id: routex-ride-request-logger`. This is the default only; both annotations specify a group ID, so it is not the active group for either current listener.

## What a Consumer Group Means

A consumer group is a logical subscription identity. Kafka tracks consumption progress independently for each group and distributes a topic's partitions among the active consumers in that group.

Within one group, a particular partition belongs to at most one active consumer at a time. One consumer may own multiple partitions. Different groups may independently consume the same topic because their offsets are tracked separately.

```text
3 partitions + 3 consumers in one group
P0 -> C1
P1 -> C2
P2 -> C3

3 partitions + 5 consumers in one group
P0 -> C1
P1 -> C2
P2 -> C3
C4 -> idle
C5 -> idle

5 partitions + 2 consumers in one group
C1 -> multiple partitions
C2 -> multiple partitions
```

For a single topic and group, the maximum useful number of active consumers is:

```text
min(number of consumers, number of partitions)
```

With RouteX's configured three partitions, no more than three consumers in either current group can actively own a partition of that one topic at once.

## Listener Concurrency Is Not Topic Partition Count

These two configuration concerns are distinct:

```text
TopicBuilder.partitions(3)
  = three Kafka logs for the topic; limits per-group partition parallelism

@KafkaListener(concurrency = "3")
  = three Spring Kafka consumer instances for that listener container
```

The active code has the first setting and no second setting. Thus it has three partitions per topic but one listener consumer instance per active listener. Adding listener concurrency would not create partitions; consumers over the partition count would be idle for that topic. Similarly, running another RouteX process in the same group changes group membership and can cause Kafka to reassign partitions.

## Offsets

An offset is a partition-local position, not a globally unique position for a topic:

```text
P0 offset 10
P1 offset 10
P2 offset 10
```

Those are three distinct positions in three logs. A consumer group's committed offset is separate from a record's offset: it represents the group's saved progress for a specific topic partition. Kafka maintains this progress independently per group, conventionally in its internal `__consumer_offsets` topic.

`auto-offset-reset: earliest` matters only when no committed offset exists for a group and partition. It directs that new group to begin at the earliest retained record. It is not a replay command for a group that already has progress.

## Current Commit Behaviour

`application.yaml` sets `spring.kafka.listener.ack-mode: manual`. Both current listener methods receive an `Acknowledgment` parameter and call `acknowledge()` after their normal processing. That makes the acknowledgement point visible in application code rather than relying on the default acknowledgement mode.

Manual acknowledgement is not a complete delivery policy. `DriverMatchingConsumer` publishes `DriverAssignmentEvent` before its acknowledgement and deliberately throws for `passengerId` `failure-test`. If the original `ride-requested` record is redelivered, the assignment publish can happen again. No application retry, DLT, transaction, or idempotency mechanism changes that outcome. See [consumer observability and delivery scope](consumer-observability-and-delivery-scope.md).

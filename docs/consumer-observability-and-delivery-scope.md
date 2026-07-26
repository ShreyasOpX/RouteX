# Consumer Observability and Delivery Scope

## Active Consumer Observability

The current matching listener requests `KafkaHeaders.RECEIVED_PARTITION` and `KafkaHeaders.OFFSET`, then prints:

```text
MATCHING | ride=<rideId> | partition=<partition> | offset=<offset>
```

The current notification listener prints an assignment summary and then a partition line. Its literal second log prefix is `MATCHING`, although it is emitted by `DriverAssignmentNotificationConsumer`:

```text
NOTIFICATION: Passenger <passengerId> - Driver <driverName> (<vehicleNumber>) has been assigned to ride <rideId>
MATCHING | ride=<rideId> | partition=<partition>
```

This makes the ride correlation ID and partition visible while exercising the application. It can help show which partition a keyed ride event reached, especially when comparing several requests in Kafka UI.

The checked-out code does **not** log topic name, thread name, consumer/client ID, listener instance identity, group ID, timestamp, or a rebalance event. It has console output, not metrics, tracing, or monitoring infrastructure. The partition label in the notification consumer is also potentially misleading and should be read according to the source above.

## What Is Not Implemented Now

The active application has manual acknowledgement, configured as `spring.kafka.listener.ack-mode: manual`. Both listeners receive an `Acknowledgment` and invoke `acknowledge()` after normal processing. It does not have the following explicit mechanisms:

- retry or backoff configuration;
- a `DefaultErrorHandler` or custom error handler;
- dead-letter topic / dead-letter publishing;
- idempotency or duplicate-side-effect protection;
- Kafka transactions or exactly-once processing;
- producer send-result handling or callbacks;
- consumer rebalance listeners;
- replication configuration for the two application topics.

`DriverMatchingConsumer` intentionally throws when `event.passengerId()` is `failure-test`. It does so after it publishes the assignment event and before `acknowledge()`. This is a useful failure boundary to study, but it is not a retry policy: without idempotency or a transaction, a redelivery can produce another assignment event.

## Implementation Evolution in Git History

The learning steps are retained here by concept rather than chronological labels:

| Commit | Concept now present in the checked-out source |
| --- | --- |
| `935a494` | Initial `ride-requested` producer-to-consumer path. |
| `ee307dd` / `e38e649` / `5328652` | Matching, chained `driver-assigned` event, and notification consumption. |
| `61d314c` / `e88cb47` | Explicit consumer groups, three partitions, and partition visibility. |
| `4ab51ed` | Matching-listener offset visibility. |
| `93ddec8` | Manual acknowledgement and the simulated failure before matching acknowledgement. |

An acknowledgement lets application code signal successful handling under the configured manual acknowledgement mode. It is not idempotency, a transaction, or an exactly-once guarantee. Likewise, the logged offset identifies a partition-local record position; it is not globally unique and is not the same as a group's committed offset.

## Safe Next Observations

With the current source, submit requests with different generated ride IDs and inspect:

1. the partition printed by each listener;
2. the records and partition metadata in Kafka UI; and
3. the consumer groups `driver-matching-group` and `notification-group` in Kafka UI.

This demonstrates topic storage, key-based partition selection, and separate group progress without claiming unimplemented reliability features.

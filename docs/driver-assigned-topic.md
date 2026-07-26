# `driver-assigned` Topic

## Purpose

`driver-assigned` represents the outcome of matching: a particular driver has been assigned to a particular ride. It separates matching from the component that informs the passenger.

## Event Structure

`DriverAssignmentEvent` is a Java record in `com.routex.matching`:

| Field | Meaning |
| --- | --- |
| `rideId` | Correlates the assignment to the request and is the Kafka key. |
| `passengerId` | Passenger whose ride was assigned. |
| `driverId` | Identifier of the selected demo driver. |
| `driverName` | Selected driver's name. |
| `vehicleNumber` | Selected driver's vehicle number. |
| `assignedAt` | `Instant` created by `DriverAssignmentService`. |

## Producer

`DriverMatchingConsumer` first calls `DriverAssignmentService.assignDriver`. The service owns the simple domain decision: it randomly chooses from the in-memory `D101`, `D102`, and `D103` drivers and returns an event. It has no Kafka dependency.

`DriverAssignmentProducer.publish` owns the messaging boundary and sends:

```java
kafkaTemplate.send(KafkaTopicConfiguration.DRIVER_ASSIGNED_TOPIC, event.rideId(), event);
```

Its declared template type is `KafkaTemplate<String, Object>`. The configured JSON value serializer serializes the `DriverAssignmentEvent` value.

## Message Key and Partitioning

The producer again uses `rideId` as the key. `KafkaTopicConfiguration.driverAssignedTopic()` declares three partitions. Related assignment records for one ride can therefore remain ordered in one partition of this topic. Partition choice is independent per topic: the implementation uses the same key but does not guarantee that a given ride has the same numeric partition on both topics.

## Consumer

`DriverAssignmentNotificationConsumer.handleDriverAssigned` subscribes to this topic in `notification-group`. It prints a console message containing the passenger ID, driver name, vehicle number, and ride ID. It is a demonstration notification consumer; it does not send email, SMS, or push messages.

It also prints the received partition. That second line currently begins with `MATCHING`, which is the literal source text and should not be interpreted as a second matching consumer.

## Consumer Group and Concurrency

`notification-group` is the effective group, not `routex-ride-request-logger`. No listener concurrency is configured, so this listener uses the default concurrency of one consumer instance. That one instance can receive all three partitions. For this topic, up to three consumers in the same group can be useful at once.

## Processing Flow and Failure Scope

```text
DriverMatchingConsumer
  -> DriverAssignmentService
  -> DriverAssignmentProducer
  -> driver-assigned (key = rideId)
  -> DriverAssignmentNotificationConsumer (notification-group)
  -> console output
```

The listener receives an `Acknowledgment` and calls `acknowledge()` after printing because `spring.kafka.listener.ack-mode` is `manual`. There is no configured retry, DLT, error handler, or idempotent notification protection. The producer does not inspect the asynchronous send result. Accordingly, documentation should not treat the console line as a durable external notification acknowledgement.

# Kafka Foundations in RouteX

## Why Kafka is in RouteX

The HTTP endpoint should record the fact that a passenger requested a ride without directly invoking every downstream concern. In RouteX, the controller publishes `RideRequestedEvent`; matching and notification are reached through Kafka topic boundaries. This makes the producer independent of a consumer's Java method, runtime address, and processing speed.

The earliest implementation in Git history had this smaller path:

```text
RideRequestController -> KafkaTemplate -> ride-requested -> RideRequestedEventConsumer -> console
```

The original `RideRequestedEventConsumer` was subsequently replaced by `DriverMatchingConsumer`, and the workflow now publishes a second event. The lesson remains: a producer writes a record to the broker, then a consumer reads it later; it is not a direct method call.

## RouteX's Local Kafka Infrastructure

`compose.yaml` starts one Confluent Kafka 7.9.0 container named `routex-kafka` and Kafka UI. The Kafka process has both `broker` and `controller` roles for local KRaft operation.

| Client location | Broker address |
| --- | --- |
| RouteX running on the host | `localhost:9092` |
| Kafka UI running in Compose | `kafka:29092` |

`spring.kafka.bootstrap-servers` is `localhost:9092`. A bootstrap server is the first address the client contacts to obtain cluster metadata; it is not a promise that every later broker connection uses that one address. This local setup has only one broker. `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` is `1`, and no Compose volume is declared for broker data.

`spring.kafka.admin.fail-fast: true` makes application startup fail when the Kafka admin client cannot reach the broker. `KafkaTopicConfiguration` exposes `NewTopic` beans, allowing Spring's Kafka admin support to create the declared topics when needed.

## Records, Producers, and Consumers

A Kafka record can be understood as:

```text
ProducerRecord
├── topic
├── partition          selected by Kafka/partitioner unless supplied
├── key
├── value
└── headers
```

RouteX supplies the topic, `rideId` key, and event value. It does not explicitly select a partition or add headers. Kafka stores serialized bytes, not Java records.

| Concern | RouteX implementation |
| --- | --- |
| HTTP producer | `RideRequestController` |
| Chained-event producer | `DriverAssignmentProducer` |
| Producer abstraction | `KafkaTemplate` |
| Listener abstraction | `@KafkaListener` methods |
| Broker | the Kafka Compose service |
| Topic declarations | `KafkaTopicConfiguration` |
| Consumers | `DriverMatchingConsumer`, `DriverAssignmentNotificationConsumer` |

Spring Kafka creates listener containers around Kafka consumer clients. The containers poll, deserialize records, invoke the annotated methods, and manage the normal listener lifecycle.

## Serialization and Configuration

The active YAML configuration uses `StringSerializer` for keys and Spring Kafka's `JsonSerializer` for values. Consumers use the matching string and JSON deserializers. JSON type metadata is accepted only from `com.routex.dispatch` and `com.routex.matching`, which covers the two event packages.

`auto-offset-reset: earliest` applies when a consumer group has no committed position for an assigned partition. It does not force a group with an existing committed position to reread everything. The configured default consumer group is `routex-ride-request-logger`, but both active listeners override it with their own explicit group IDs.

## Event-Driven Communication

Kafka separates these responsibilities:

```text
dispatch publishes a fact
        ↓
matching reacts and publishes a new fact
        ↓
notification reacts to the new fact
```

This creates room for another independent group—such as analytics—to consume a topic later without modifying the existing producer. RouteX does not currently contain such a consumer, but that is the architectural property the topic boundary demonstrates.

For current topic contracts, see [ride-requested](ride-requested-topic.md) and [driver-assigned](driver-assigned-topic.md).

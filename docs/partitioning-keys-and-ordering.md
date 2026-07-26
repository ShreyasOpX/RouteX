# Partitioning, Keys, and Ordering

## RouteX Configuration

`KafkaTopicConfiguration` declares three partitions for each current topic:

```text
ride-requested                 driver-assigned
├── partition 0                ├── partition 0
├── partition 1                ├── partition 1
└── partition 2                └── partition 2
```

`TopicBuilder.partitions(3)` sets the requested topic partition count. It is unrelated to `@KafkaListener` concurrency. If a topic already exists, inspect Kafka UI or broker metadata to confirm its effective runtime partition count; partition configuration cannot shrink an existing Kafka topic.

## Partitions as Ordered Logs

A topic is split into partitions. Each partition is an append-only, ordered log with its own offset sequence:

```text
partition 0: offset 0 -> offset 1 -> offset 2 -> ...
partition 1: offset 0 -> offset 1 -> offset 2 -> ...
partition 2: offset 0 -> offset 1 -> offset 2 -> ...
```

Kafka preserves ordering within one partition. It does not provide a single global ordering across all three partitions. Multiple ride IDs may share one partition, and unrelated rides in different partitions can be processed in parallel.

## Why RouteX Uses `rideId` as a Key

Both producers call `KafkaTemplate.send(topic, event.rideId(), event)`. The record key is a string `rideId`:

```text
same rideId
  -> same serialized key
  -> consistent partition selection for that topic's current metadata
  -> records for that ride remain in one partition
  -> partition-local ordering is available
```

The producer does not set `ProducerRecord.partition`; Kafka's partitioning logic selects it. This is valuable when RouteX later emits more events for a ride to the same topic: their relative order can be maintained without serializing all rides through one partition.

There are important limits:

- This is ordering inside one topic partition, not a cross-topic transaction or global workflow order.
- A numeric partition from `ride-requested` should not be assumed to equal the numeric partition from `driver-assigned`, even with the same key.
- Changing a topic's partition count can change where future records for a key are mapped. Existing records remain in their old partitions, so do not use a partition-count change casually when strict per-key ordering across the transition matters.
- Kafka's ordering guarantee does not make application side effects idempotent or remove the need to reason about retries.

## Parallelism Objective

RouteX's target trade-off is:

```text
parallelism between different rides
            +
ordering for records of the same ride within a topic partition
```

Three partitions permit three independently assigned partitions to be consumed concurrently within one group, provided there are enough consumer instances. The active application has only one instance per listener, so it currently demonstrates partitioned storage and assignment visibility more than parallel listener execution.

## Records and Headers

Conceptually, every Kafka record includes topic, partition, key, value, offset, timestamp, and optional headers. RouteX explicitly supplies topic, key, and value. Both current listeners request the received partition header; `DriverMatchingConsumer` also requests the received offset header. They do not explicitly add application headers.

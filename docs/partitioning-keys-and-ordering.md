# Partitioning, Message Keys, and Ordering

## RouteX Partition Strategy

Every application topic declared by `KafkaTopicConfiguration` has 3 partitions:

```text
ride-requested        driver-assigned        ride-requested-dlt
|- partition 0        |- partition 0         |- partition 0
|- partition 1        |- partition 1         |- partition 1
`- partition 2        `- partition 2         `- partition 2
```

A partition is an ordered append-only log. Each partition has an independent offset sequence, so partition 0 offset 5 and partition 1 offset 5 are different records. Partitions enable different rides to be processed in parallel; Kafka does not provide global ordering across all partitions.

Both RouteX producers call `KafkaTemplate.send(topic, event.rideId(), event)`. No producer explicitly sets a partition. Kafka's partitioner uses the serialized `rideId` key and current topic partition metadata:

```text
same rideId -> same key -> same selected partition in that topic
            -> order preserved within that partition
```

Several ride IDs can share a partition. The same key does not mean the same numeric partition across different topics, and adding partitions can change the selection for future records while leaving older records where they are.

## Practical Verification: Partitioning Experiment

### Prerequisites

Start Docker Kafka and RouteX as shown in the [README](../README.md#run-locally).

### Trigger

Send several requests; each receives a different generated `rideId`, which is the Kafka key.

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-201","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-202","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-203","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
```

### Expected Output

For each generated ride ID, matching prints this shape:

```text
MATCHING | ride=<generated-uuid> | partition=<0|1|2> | offset=<dynamic-offset>
```

### What This Demonstrates

Observe `rideId`, `partition`, and `offset`. Different UUIDs can land in different partitions, but a small sample can also place several rides in one partition. The source does not promise a particular partition for a particular generated UUID. Kafka UI can confirm the records under `ride-requested` partitions.

## Ordering Experiment

The public endpoint always generates a new ride ID, so it cannot submit two HTTP requests with the same key. The code nevertheless shows the ordering rule: both producer calls use their event's `rideId` key. If a future producer emitted multiple records for one ride to the same topic without a partition-count change, Kafka would append them to one partition in send order. This is partition-local ordering, not an end-to-end or cross-topic ordering guarantee.

## Key Takeaways

- Partitions are ordered logs and enable parallelism.
- `rideId` is RouteX's key and influences partition selection.
- Kafka guarantees order within a partition, never one global order over a multi-partition topic.

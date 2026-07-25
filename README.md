# RouteX

## Event-Driven Ride Dispatch with Spring Boot and Apache Kafka

RouteX is a deliberately incremental Kafka learning project built with Spring Boot. It uses a ride-dispatch domain because the domain naturally produces multiple independent business facts:

- a passenger requests a ride
- matching assigns a driver
- notification informs the passenger
- future systems could price, track, analyze, or detect fraud

The business logic is intentionally simple so Kafka behavior stays visible.

This README is designed to be:

- project documentation
- a Kafka learning guide from first principles
- a long-term revision reference
- a teaching document for another backend student
- an interview-preparation aid
- a record of what RouteX actually implements today

Source of truth: the repository, not the older README history.

## 1. Project purpose

The goal of RouteX is not to simulate a production ride-hailing platform in full. The goal is to learn how Kafka works underneath Spring abstractions by implementing one concept at a time:

- publishing an event from HTTP
- storing it in Kafka
- consuming it asynchronously
- chaining a second business event
- separating logical responsibilities with topics
- introducing partitions and consumer groups

That is why RouteX keeps driver matching in memory, prints notifications to the console, and uses a single local Kafka broker in Docker.

## 2. What we are learning

| Phase | Focus | Status in current repository |
| --- | --- | --- |
| Phase 1 | Producer -> broker -> topic -> consumer fundamentals | Implemented |
| Phase 2 | Event chaining and domain boundaries | Implemented |
| Phase 3 | Consumer groups, partitions, offsets basics, partition visibility | Current |
| Future | Retries, DLT/DLQ, acknowledgements, delivery semantics, idempotency, transactions, schema evolution, observability, replication, security | Planned |

What “Current” means here: the code already has explicit listener group IDs and 3 partitions on both topics, but it does not yet implement multi-instance experiments, rebalance observation code, retries, or advanced delivery controls.

## 3. Current architecture

### Runtime flow

```text
PowerShell / HTTP client
        |
        v
POST /rides/requests
        |
        v
RideRequestController
        |
        v
RideRequestedEvent
        |
        v
KafkaTemplate<String, RideRequestedEvent>
        |
        v
KafkaProducer
        |
        v
JsonSerializer -> bytes
        |
        v
Kafka broker
        |
        v
Topic: ride-requested (3 partitions)
        |
        v
DriverMatchingConsumer
        |
        v
DriverAssignmentService
        |
        v
DriverAssignmentEvent
        |
        v
DriverAssignmentProducer
        |
        v
KafkaTemplate<String, Object>
        |
        v
KafkaProducer
        |
        v
JsonSerializer -> bytes
        |
        v
Kafka broker
        |
        v
Topic: driver-assigned (3 partitions)
        |
        v
DriverAssignmentNotificationConsumer
        |
        v
Console output
```

### Logical boundaries

```text
dispatch boundary
  - RideRequestController
  - RideRequest
  - RideRequestedEvent
  - KafkaTopicConfiguration

matching boundary
  - Driver
  - DriverAssignmentService
  - DriverAssignmentEvent
  - DriverMatchingConsumer
  - DriverAssignmentProducer

notification boundary
  - DriverAssignmentNotificationConsumer

Kafka boundary
  - ride-requested topic
  - driver-assigned topic
  - partitions
  - offsets
  - consumer groups
  - broker storage
```

Why these boundaries matter:

- dispatch does not call notification directly
- matching does not need notification’s Java type or method
- notification does not need HTTP knowledge
- Kafka is the shared boundary between independent responsibilities

This is the core event-driven idea in RouteX.

## 4. Repository audit summary

The current repository contains:

- Spring Boot `3.5.16`
- Java target `21`
- one Kafka broker via Docker Compose
- Kafka UI on port `8081`
- two topics: `ride-requested`, `driver-assigned`
- `3` partitions configured for each topic
- one HTTP producer endpoint: `POST /rides/requests`
- one matching consumer group: `driver-matching-group`
- one notification consumer group: `notification-group`
- global Spring consumer default group ID: `routex-ride-request-logger`

Important: the two `@KafkaListener` methods override the global consumer group with explicit `groupId` values, so the effective listener groups are:

- `driver-matching-group`
- `notification-group`

## 5. Package and class map

### `com.routex`

- `RouteXApplication`

### `com.routex.dispatch`

- `KafkaTopicConfiguration`
- `RideRequest`
- `RideRequestedEvent`
- `RideRequestController`

### `com.routex.matching`

- `Driver`
- `DriverAssignmentEvent`
- `DriverAssignmentService`
- `DriverMatchingConsumer`
- `DriverAssignmentProducer`

### `com.routex.notification`

- `DriverAssignmentNotificationConsumer`

## 6. Running RouteX

### Infrastructure

```powershell
docker compose up -d
```

### Verify Kafka connectivity from the host

```powershell
Test-NetConnection localhost -Port 9092
```

You want:

```text
TcpTestSucceeded : True
```

### Start the application

```powershell
mvn spring-boot:run
```

### Ports

| Port | Component | Meaning |
| --- | --- | --- |
| `8080` | RouteX Spring Boot app | HTTP API |
| `9092` | Kafka broker host listener | Kafka protocol over TCP |
| `8081` | Kafka UI | Browser inspection tool |

### Kafka UI

Open:

```text
http://localhost:8081
```

### PowerShell test requests

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-101","pickupLocation":"Koramangala","destinationLocation":"Indiranagar"}'
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-102","pickupLocation":"HSR Layout","destinationLocation":"MG Road"}'
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-103","pickupLocation":"Whitefield","destinationLocation":"Electronic City"}'
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-104","pickupLocation":"Jayanagar","destinationLocation":"Hebbal"}'
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/rides/requests" -ContentType "application/json" -Body '{"passengerId":"passenger-105","pickupLocation":"Banashankari","destinationLocation":"Bellandur"}'
```

### Expected HTTP response

The controller returns `202 Accepted` with a JSON body equivalent to:

```json
{
  "rideId": "generated-uuid",
  "passengerId": "passenger-103",
  "pickupLocation": "Koramangala",
  "destinationLocation": "Indiranagar",
  "requestedAt": "2026-07-25T..."
}
```

Important: `202 Accepted` means the HTTP layer accepted the request and handed the event to the producer client. It does not prove the full downstream Kafka workflow already completed.

### Expected application logs

Current source code prints:

```text
MATCHING | ride=<rideId> | partition=<n>
NOTIFICATION: Passenger <passengerId> - Driver <driverName> (<vehicleNumber>) has been assigned to ride <rideId>
MATCHING | ride=<rideId> | partition=<n>
```

Notes:

- the first `MATCHING` line comes from `DriverMatchingConsumer`
- the `NOTIFICATION` line comes from `DriverAssignmentNotificationConsumer`
- the second `MATCHING` line is also printed by the notification consumer in the current code, even though the label says `MATCHING`
- actual partition numbers vary

## 7. Kafka fundamentals from first principles

### Event

What:

An event is an immutable fact about something that already happened.

Why:

Events let systems react asynchronously without forcing the original caller to know every downstream dependency.

RouteX:

- `RideRequestedEvent` means a ride request was accepted
- `DriverAssignmentEvent` means matching assigned a driver

Internals:

Kafka does not store Java objects. It stores bytes. Events become bytes through serialization before they are written to Kafka.

### Producer

What:

A producer writes records to Kafka.

Why:

The producer is the boundary between application logic and Kafka storage.

RouteX:

- `RideRequestController` publishes `RideRequestedEvent`
- `DriverAssignmentProducer` publishes `DriverAssignmentEvent`

Internals:

In RouteX, application code calls `KafkaTemplate.send(...)`. Spring Kafka delegates that to the Kafka Java producer client (`KafkaProducer`), which serializes the key and value and sends them over Kafka’s protocol to the broker.

### KafkaTemplate

What:

`KafkaTemplate` is Spring Kafka’s producer abstraction.

Why:

It reduces boilerplate and integrates cleanly with Spring dependency injection.

RouteX:

- `KafkaTemplate<String, RideRequestedEvent>` in `RideRequestController`
- `KafkaTemplate<String, Object>` in `DriverAssignmentProducer`

Internals:

`KafkaTemplate.send(topic, key, value)` ultimately creates a producer record and delegates to the Kafka client library. The send is asynchronous unless code explicitly waits on the returned future.

### Broker

What:

A broker is a Kafka server process that receives, stores, and serves records.

Why:

It decouples producers from consumers. Producers publish to Kafka, not to specific consumers.

RouteX:

The Docker Compose file starts one Kafka broker using `confluentinc/cp-kafka:7.9.0`.

Internals:

The broker stores topic partitions as append-only logs on disk. Consumers later fetch records from those logs by partition and offset.

### Topic

What:

A topic is a named category of records.

Why:

It separates different streams of business facts.

RouteX:

- `ride-requested`
- `driver-assigned`

Internals:

Each topic is split into partitions. In the current repository, both topics are declared with `3` partitions in `KafkaTopicConfiguration`.

### Record / message

What:

A Kafka record contains:

- topic
- partition
- offset
- key bytes
- value bytes
- metadata such as timestamp

Why:

This is the unit Kafka stores and consumers read.

RouteX:

Each `RideRequestedEvent` and `DriverAssignmentEvent` becomes one Kafka record.

Internals:

Application code sees Java objects before serialization and after deserialization. Kafka itself persists bytes plus metadata.

### Key

What:

The key is metadata attached to a record.

Why:

It can influence partition selection and is often used to keep related records together.

RouteX:

Both producer paths use `rideId` as the Kafka key.

Internals:

For keyed records, the producer uses the key to choose a partition consistently while the topic’s partitioning setup remains stable. Kafka guarantees ordering within a partition, not across the whole topic.

### Serialization and deserialization

What:

Serialization turns Java objects into bytes. Deserialization turns bytes back into Java objects.

Why:

Networks and disks do not store Java records directly.

RouteX:

- producer value serializer: `JsonSerializer`
- consumer value deserializer: `JsonDeserializer`

Internals:

The serializers convert RouteX event objects to JSON bytes before they leave the process. On the consumer side, bytes are read from Kafka and converted back into Java records before listener invocation.

### Consumer

What:

A consumer reads records from Kafka.

Why:

It lets downstream logic react asynchronously and independently.

RouteX:

- `DriverMatchingConsumer` consumes `ride-requested`
- `DriverAssignmentNotificationConsumer` consumes `driver-assigned`

Internals:

Spring Kafka does not make `@KafkaListener` itself poll. Spring creates listener containers. Those containers manage Kafka consumer clients, subscribe them to topics, call `poll()`, deserialize records, and invoke your listener method.

### Kafka UI

What:

Kafka UI is a separate application for inspecting Kafka state.

Why:

It helps you verify what the broker actually contains.

RouteX:

Kafka UI is started by Docker Compose and exposed on `localhost:8081`.

Internals:

Kafka UI talks to Kafka as an external client. It is not the broker and it does not make your application work.

## 8. One `RideRequestedEvent` end-to-end

This is the most important mental model in RouteX:

1. A PowerShell client sends `POST /rides/requests`.
2. Spring MVC maps the JSON body to `RideRequest`.
3. Bean validation checks the `@NotBlank` fields.
4. `RideRequestController` creates a new `RideRequestedEvent`.
5. The controller calls `KafkaTemplate.send("ride-requested", rideId, event)`.
6. Spring Kafka delegates to the Kafka producer client.
7. The string key and JSON value are serialized to bytes.
8. The producer sends those bytes to the broker over Kafka’s TCP protocol.
9. The broker appends the record to one partition of `ride-requested`.
10. The record gets a partition-relative offset in that partition log.
11. `DriverMatchingConsumer`’s listener container polls Kafka.
12. Kafka returns consumer records from the assigned partition(s).
13. Spring Kafka deserializes the value bytes into `RideRequestedEvent`.
14. Spring resolves the `@Header(KafkaHeaders.RECEIVED_PARTITION)` parameter.
15. Spring invokes `handleRideRequested(...)` on the listener bean.
16. `DriverAssignmentService` creates a `DriverAssignmentEvent`.
17. `DriverAssignmentProducer` publishes that new event to `driver-assigned`.
18. Kafka stores the second record in one partition of `driver-assigned`.
19. `DriverAssignmentNotificationConsumer` polls `driver-assigned`.
20. Spring deserializes the assignment event and calls the notification listener.
21. The notification consumer prints the assignment to the console.

Critical correction:

- producer -> Kafka
- consumer <- Kafka

The producer does not send directly to the consumer.

Also important:

- a producer can publish even if no consumer is currently running
- consuming a record does not inherently delete it from Kafka
- if Kafka has already stored the record, the record can survive an application crash
- if the JVM crashes before publish succeeds, an in-memory Java object disappears with the process

## 9. Spring Kafka internals

### Producer side

```text
RouteX code
  -> KafkaTemplate
  -> KafkaProducer
  -> key/value serializers
  -> Kafka protocol over TCP
  -> broker
```

Pieces by ownership:

| Layer | Owned by |
| --- | --- |
| `RideRequestController`, `DriverAssignmentProducer` | RouteX |
| `KafkaTemplate` | Spring Kafka |
| `KafkaProducer` and Kafka protocol | Apache Kafka client library |
| broker storage | Apache Kafka broker |

### Consumer side

```text
broker
  -> KafkaConsumer.poll()
  -> ConsumerRecord
  -> deserializer
  -> Spring listener container
  -> @KafkaListener method
  -> RouteX business logic
```

Pieces by ownership:

| Layer | Owned by |
| --- | --- |
| broker, partitions, offsets | Apache Kafka |
| `KafkaConsumer` | Apache Kafka client library |
| listener container, method invocation | Spring Kafka |
| `DriverMatchingConsumer`, `DriverAssignmentNotificationConsumer`, `DriverAssignmentService` | RouteX |

What happens under `@KafkaListener`:

1. Spring scans beans and finds listener annotations.
2. Spring Kafka creates listener container infrastructure.
3. The container creates and manages a `KafkaConsumer`.
4. The consumer joins a consumer group.
5. Kafka assigns partitions for subscribed topics.
6. A consumer thread calls `poll()`.
7. Kafka returns `ConsumerRecord` batches.
8. Headers, key, value, partition, and offset are available.
9. Spring deserializes the value.
10. Spring resolves listener parameters such as the event object and partition header.
11. Spring invokes the annotated method.

## 10. Phase 1 — Kafka fundamentals in RouteX

Phase 1 in RouteX is the producer-to-broker-to-consumer path for a single business fact: a passenger requested a ride.

### What was implemented

- HTTP endpoint `POST /rides/requests`
- `RideRequest` request body record
- `RideRequestedEvent` event record
- `ride-requested` topic
- Kafka publication from the controller
- downstream consumption by the matching listener

### Why this phase matters

Before learning consumer groups or rebalancing, you need the base model:

- Kafka is not a direct method call
- records are stored, then later consumed
- Spring annotations sit on top of Kafka clients

### RouteX mapping

| Concept | RouteX |
| --- | --- |
| producer | `RideRequestController` |
| event | `RideRequestedEvent` |
| topic | `ride-requested` |
| consumer | `DriverMatchingConsumer` |
| deserialized payload type | `RideRequestedEvent` |

### What happens underneath

The controller does not “send to matching.” It sends to Kafka. Matching is just one consumer group that later polls Kafka and reacts.

## 11. Phase 2 — event chaining

Phase 2 in RouteX is where the first event causes a second business fact rather than ending in a log line.

### Milestone 1: add driver assignment domain model

Implemented classes:

- `Driver`
- `DriverAssignmentEvent`
- `DriverAssignmentService`

What:

- `Driver` is the in-memory domain model for available demo drivers
- `DriverAssignmentEvent` is the fact emitted after assignment
- `DriverAssignmentService` chooses a driver and creates the event

Why:

Business logic should exist separately from Kafka APIs.

RouteX:

`DriverAssignmentService` uses a small in-memory driver list and `ThreadLocalRandom` to pick a driver.

Internals:

This class is a Spring `@Service`, so Spring instantiates it as a bean and injects it into the consumer. It has no Kafka client code, which keeps it reusable and easy to reason about.

### Milestone 2: consume ride-requested events for matching

Implemented class:

- `DriverMatchingConsumer`

What:

This class adapts Kafka input to the matching use case.

Why:

Kafka-specific listener code should stay out of business logic classes.

RouteX:

`DriverMatchingConsumer` is annotated with `@Component` and `@KafkaListener(topics = "ride-requested", groupId = "driver-matching-group")`.

Internals:

Spring Kafka creates a listener container, the container manages a Kafka consumer, the consumer joins `driver-matching-group`, receives partition assignments, polls Kafka, deserializes `RideRequestedEvent`, resolves the partition header, and invokes `handleRideRequested(...)`.

### Milestone 3: publish driver-assigned events

Implemented classes/config:

- `DriverAssignmentProducer`
- second topic constant and topic bean in `KafkaTopicConfiguration`

What:

The matching stage publishes a second business event to Kafka.

Why:

This proves consumer -> process -> producer chaining.

RouteX:

- topic: `driver-assigned`
- key: `rideId`
- value: `DriverAssignmentEvent`

Internals:

`KafkaTopicConfiguration` defines `NewTopic` beans. Spring Kafka admin support uses Kafka `AdminClient` behavior under the hood to request topic creation at startup. Publication of `DriverAssignmentEvent` then follows the same producer path as `RideRequestedEvent`: object -> serializer -> bytes -> broker -> topic partition.

Important:

Publication to `driver-assigned` does not require a downstream consumer to exist. Kafka can store the record even if no consumer is subscribed.

### Milestone 4: consume driver assignment events

Implemented class:

- `DriverAssignmentNotificationConsumer`

What:

A second independent consumer listens for assignment events and prints a notification.

Why:

This demonstrates independent downstream processing of the second event.

RouteX:

`DriverAssignmentNotificationConsumer` listens on `driver-assigned` with `groupId = "notification-group"`.

Internals:

This listener belongs to a different consumer group than matching. That means matching and notification are independent logical subscribers, not shared workers of one responsibility.

Why console output is intentional:

- it keeps the flow visible
- it avoids hiding Kafka concepts behind email/SMS integrations
- it lets Kafka UI and console logs be used together as learning tools

### Complete Phase 2 runtime trace

```text
HTTP request
  -> RideRequestController
  -> RideRequestedEvent
  -> Kafka publish to ride-requested
  -> DriverMatchingConsumer
  -> DriverAssignmentService
  -> DriverAssignmentEvent
  -> Kafka publish to driver-assigned
  -> DriverAssignmentNotificationConsumer
  -> console notification
```

## 12. Event chaining theory

### Event vs command

Event:

“This already happened.”

Command:

“Please make this happen.”

RouteX examples:

- `RideRequestedEvent`: the ride request was accepted
- `DriverAssignmentEvent`: a driver was assigned
- a hypothetical `AssignDriverCommand` would mean “please assign a driver,” but RouteX does not model commands explicitly yet

### Why this is not just a chain of Java calls

The current runtime includes Java method calls inside each stage, but the stage boundaries are Kafka topics:

```text
RideRequestedEvent
        |
        v
matching stage
        |
        v
DriverAssignmentEvent
        |
        v
notification stage
```

Matching does not call a notification method directly. It emits a new fact to Kafka. Notification independently consumes that fact later.

### Future fan-out

Conceptually, `driver-assigned` could support:

```text
driver-assigned
      |
      +--> notification
      +--> analytics
      +--> ETA
      +--> tracking
```

That is conceptual fan-out across different consumers or services. Do not confuse that with “multiple consumers in the same group share work.” Those are different ideas.

## 13. Phase 3 — consumer groups and partitions

This is the current learning phase represented in the repository.

### What currently exists

- both topics are created with `3` partitions
- `DriverMatchingConsumer` uses `groupId = "driver-matching-group"`
- `DriverAssignmentNotificationConsumer` uses `groupId = "notification-group"`
- both listeners log the received partition number

### Why consumer groups exist

Problem:

If three matching instances all independently consumed the same ride-request event as separate subscribers, they could assign multiple drivers to one ride.

Solution:

Consumers that represent the same logical responsibility join the same consumer group so Kafka can distribute partition ownership among them instead of duplicating work within that group.

### Consumer group != consumer

Consumer:

One running Kafka consumer client instance.

Consumer group:

A logical subscription identity shared by one or more consumer instances.

RouteX:

- `driver-matching-group` = matching responsibility
- `notification-group` = notification responsibility

Matching and notification are different groups because they are different logical responsibilities and should each process the same business flow independently.

### Group progress and offsets

Conceptually, Kafka tracks progress per:

```text
(consumer group, topic, partition)
```

That is why different consumer groups can both process the same topic without interfering with each other.

Kafka stores committed group offset metadata internally in Kafka, including the internal `__consumer_offsets` topic. Application developers normally inspect behavior through logs and tools; they do not manually edit that internal topic.

## 14. Partitions

### What is a partition

A topic is not one flat list. It is split into partitions:

```text
ride-requested
  ├── Partition 0
  ├── Partition 1
  └── Partition 2
```

Same idea for `driver-assigned`.

### Why Kafka partitions topics

Partitions allow:

- scalability
- parallel consumption
- partition-local ordering
- distribution of work across consumers in a group

### Critical assignment rule

Within one consumer group:

- one partition can be actively assigned to at most one consumer at a time

But:

- one consumer can own multiple partitions

Examples:

| Partitions | Consumers in same group | Result |
| --- | --- | --- |
| 3 | 1 | the single consumer may own all 3 |
| 3 | 3 | up to 3 active consumers |
| 3 | 5 | at most 3 active consumers; 2 may be idle for that topic |
| 10 | 3 | all 3 can be active; some own multiple partitions |

Assignments are not guaranteed to be perfectly equal. Exact distribution depends on Kafka’s assignor and current group state.

### RouteX use

`KafkaTopicConfiguration` explicitly requests `3` partitions for:

- `ride-requested`
- `driver-assigned`

The listeners also read the partition number via:

- `@Header(KafkaHeaders.RECEIVED_PARTITION)`

So the current application surfaces partition placement in logs.

## 15. Message keys and ordering

### Current RouteX producer behavior

Both producer paths use:

```text
key = rideId
value = event object
```

Specifically:

- controller publishes `RideRequestedEvent` with key `event.rideId()`
- assignment producer publishes `DriverAssignmentEvent` with key `event.rideId()`

### Why this matters

Using `rideId` as the key is a sensible educational choice because related records for the same ride can be routed consistently to the same partition while the partition configuration remains stable.

### Ordering guarantee

Kafka guarantees ordering within one partition.

Kafka does not guarantee global ordering across all partitions of a multi-partition topic.

That means:

- events with the same effective key-to-partition result can preserve partition order
- events in different partitions can be consumed independently and interleaved

### Important nuance

Do not rely on a simplistic hardcoded partition formula in documentation unless you verified it at the client/config level. The safe statement is:

- the key influences partition selection
- the same key generally maps consistently while the topic partition setup remains stable
- changing partition count can change future mapping behavior

## 16. Offsets

### What is an offset

An offset is a partition-relative position in the log:

```text
Partition 0
  offset 0
  offset 1
  offset 2
  offset 3
```

It is not simply “number of messages processed.”

### Three different ideas

| Concept | Meaning |
| --- | --- |
| record offset | the stored position of a record inside a partition |
| consumer position | where a consumer will read next |
| committed offset | the progress the consumer group has durably recorded |

### Why committed offsets matter

Committed offsets let a consumer group resume from previously recorded progress instead of always restarting from the beginning.

Kafka does not automatically restart every consumer from offset `0` on every application restart.

### RouteX configuration

Current config:

```yaml
spring.kafka.consumer.auto-offset-reset: earliest
```

Meaning:

- if a group has no valid committed offset for a partition, Kafka starts from the earliest available record
- if a valid committed offset already exists, `auto-offset-reset` does not override it

This distinction is important.

## 17. Rebalancing

### Concept / upcoming experiment

The current code introduces group IDs and partitions, but the repository does not yet implement multi-instance rebalance experiments. So this section is conceptual and marked upcoming.

Initial state:

```text
P0 -> Consumer A
P1 -> Consumer B
P2 -> Consumer C
```

If Consumer B dies, group membership changes and Kafka reassigns partitions. A possible new state is:

```text
P0 -> Consumer A
P1 -> Consumer A
P2 -> Consumer C
```

What triggers a rebalance:

- a consumer joins
- a consumer leaves
- a consumer is considered failed
- partition or subscription state changes in ways that require reassignment

Important:

Exact assignments depend on the assignor, protocol, and current group state. Do not assume every rebalance produces the same distribution.

## 18. Networking

### Actual ports from this repository

| Address | Meaning |
| --- | --- |
| `localhost:8080` | RouteX HTTP API |
| `localhost:9092` | Kafka broker exposed to the host |
| `localhost:8081` | Kafka UI |

### Docker listener configuration

Current `compose.yaml` config includes:

- internal Docker-network listener: `kafka:29092`
- controller listener: `kafka:29093`
- host listener: `0.0.0.0:9092`
- advertised host listener: `localhost:9092`

Why this exists:

- RouteX running on your host needs a broker address it can reach: `localhost:9092`
- Kafka UI running as a container needs a Docker-network address it can reach: `kafka:29092`

Kafka communication here is not REST. It is Kafka’s own protocol over TCP.

## 19. Kafka UI

### What Kafka UI is

Kafka UI is an inspection and administration application.

### What Kafka UI is not

- it is not the Kafka broker
- it does not replace the producer or consumer clients
- it does not prove your application logic is correct on its own

### What to inspect in Kafka UI

- topics
- partitions
- messages
- keys
- values
- offsets
- consumer groups
- lag, if shown by the UI

### Why it matters for learning

Kafka UI lets you verify broker state directly instead of assuming Spring annotations worked just because the app printed a log line.

Useful checks:

- confirm `ride-requested` received the request event
- confirm `driver-assigned` received the assignment event
- compare keys and values
- inspect which partitions are receiving records
- inspect consumer groups independently of application logs

## 20. Kafka experiments

### Experiment 1: producer publishes to Kafka, not directly to a consumer

Hypothesis:

The producer path and consumer path are decoupled by Kafka.

Steps:

1. Start Kafka.
2. Start RouteX.
3. Send a ride request.
4. Inspect `ride-requested` in Kafka UI.

Expected observation:

The event exists in Kafka as a stored record.

What it proves:

The producer publishes to Kafka storage, not to a consumer method directly.

### Experiment 2: trace one `rideId` across both topics

Hypothesis:

One accepted ride request becomes one assignment event with the same `rideId`.

Steps:

1. Send one POST request.
2. Copy the returned `rideId`.
3. Inspect `ride-requested`.
4. Inspect `driver-assigned`.

Expected observation:

The same `rideId` appears in both topics.

What it proves:

Event chaining is happening through Kafka topic boundaries.

### Experiment 3: observe partition metadata in logs

Hypothesis:

Records are being stored in partitions, not in one unstructured global queue.

Steps:

1. Send several ride requests.
2. Watch RouteX console logs.
3. Inspect partition information in Kafka UI.

Expected observation:

The application logs partition numbers and Kafka UI shows topic partitions.

What it proves:

Kafka is distributing records across topic partitions, and Spring listener methods can receive partition metadata.

### Experiment 4: observe independent consumer groups

Hypothesis:

Matching and notification are separate logical subscribers.

Steps:

1. Start RouteX.
2. Open Kafka UI consumer-group view.
3. Inspect groups associated with the topics.

Expected observation:

You should see separate logical groups for matching and notification behavior.

What it proves:

Different groups can independently process related records.

### Experiment 5: Kafka retains records after consumption

Hypothesis:

Consuming a record does not inherently delete it from Kafka immediately.

Steps:

1. Send a ride request.
2. Observe matching and notification logs.
3. Open Kafka UI and inspect the topic messages afterward.

Expected observation:

The records are still visible in Kafka UI after consumption.

What it proves:

Kafka is a log of stored records, not a direct ephemeral method-dispatch mechanism.

### Upcoming experiment: multiple RouteX instances in the same group

Hypothesis:

Matching work would be shared across instances of the same consumer group.

Status:

Upcoming, not implemented or verified in this repository yet.

### Upcoming experiment: kill one instance and observe rebalance

Hypothesis:

Partition ownership would be reassigned when group membership changes.

Status:

Upcoming, not implemented or verified in this repository yet.

## 21. Debugging guide

| Symptom | Likely layer | What to check |
| --- | --- | --- |
| HTTP request fails before `202` | Spring MVC / app startup | Is RouteX running on `8080`? Is the JSON body valid? Are validation errors being returned? |
| HTTP works but `ride-requested` is empty | producer / broker connectivity | Check broker startup, `localhost:9092`, RouteX startup logs, and whether `KafkaTemplate.send(...)` reached Kafka |
| App fails at startup with Kafka timeout | Kafka admin / broker | Check `docker compose ps`, broker logs, `Test-NetConnection localhost -Port 9092`, and advertised listener config |
| `ride-requested` populated but `driver-assigned` empty | matching listener or producer path | Check `DriverMatchingConsumer`, `DriverAssignmentService`, `DriverAssignmentProducer`, and listener exceptions |
| `driver-assigned` populated but no notification line | notification consumer | Check `DriverAssignmentNotificationConsumer`, consumer group visibility, and deserialization/logging issues |
| Deserialization failure | serializer/deserializer/trusted packages | Verify event class names, JSON type handling, and `spring.json.trusted.packages` |
| Consumer group not visible | app runtime / group join timing | Ensure the listener is actually running and subscribed; inspect Kafka UI after the app is fully started |
| Duplicate processing or duplicate-looking logs | runtime topology / offsets / logging | Check whether multiple app instances are running, whether different groups are expected to process the same record, whether records were redelivered, or whether duplicated logging lines come from separate listeners |
| Consumer appears idle | offsets / group assignment / no new records | Check whether the group already has committed offsets, whether `auto-offset-reset` applies, and whether the consumer owns partitions |
| Partition distribution seems unexpected | producer keying / partition count / assignor expectations | Remember partition placement is influenced by the key and configuration; do not assume perfect spread on small samples |

Evidence-first debugging rule:

Inspect broker state, logs, topic records, group state, and configuration before changing code.

## 22. Common beginner misconceptions

### “Producer sends directly to consumer.”

Wrong mental model:

Controller -> listener method

Correct mental model:

Controller -> Kafka -> listener later polls Kafka

RouteX example:

`RideRequestController` publishes to `ride-requested`; `DriverMatchingConsumer` later reads from Kafka.

### “Kafka removes a message after it is consumed.”

Wrong mental model:

Consumption deletes the record immediately.

Correct mental model:

Kafka keeps records according to retention rules; consumer progress is tracked separately.

RouteX example:

You can still inspect consumed records in Kafka UI.

### “`@KafkaListener` itself continuously polls.”

Wrong mental model:

The annotation is the consumer loop.

Correct mental model:

Spring Kafka creates a listener container, and the container manages a Kafka consumer that polls.

RouteX example:

The listener methods are callbacks invoked by Spring Kafka infrastructure.

### “Kafka stores Java objects.”

Wrong mental model:

Kafka persists Java records directly.

Correct mental model:

Kafka stores bytes; Java objects are serialized before storage and deserialized after reading.

RouteX example:

`RideRequestedEvent` and `DriverAssignmentEvent` are converted to JSON bytes.

### “One topic equals one queue.”

Wrong mental model:

A topic is one simple linear queue with one global consumer flow.

Correct mental model:

A topic contains partitions and can be consumed independently by different consumer groups.

RouteX example:

`driver-assigned` can conceptually feed notification, analytics, and other consumers.

### “More consumers always means more throughput.”

Wrong mental model:

Adding consumers always increases active parallel work.

Correct mental model:

Within one group, active consumption for a topic is limited by partition assignments.

RouteX example:

With 3 partitions, 5 consumers in one group cannot all be active on that topic.

### “Consumer and consumer group mean the same thing.”

Wrong mental model:

They are interchangeable.

Correct mental model:

A consumer is one client instance; a consumer group is the logical subscription identity.

RouteX example:

`driver-matching-group` is the group, not the listener method itself.

### “Offsets belong globally to the topic.”

Wrong mental model:

There is one universal topic offset.

Correct mental model:

Offsets are per partition.

RouteX example:

Partition `0` and partition `1` each have their own independent offset sequences.

### “Ordering is guaranteed across the whole topic.”

Wrong mental model:

Kafka preserves one global order across all partitions.

Correct mental model:

Kafka guarantees ordering within a partition.

RouteX example:

Two rides stored in different partitions can be observed independently.

### “Kafka UI is Kafka.”

Wrong mental model:

The browser tool is the broker.

Correct mental model:

Kafka UI is an external inspection tool.

RouteX example:

The broker runs on `9092`; Kafka UI runs separately on `8081`.

### “HTTP 202 means the entire Kafka workflow completed.”

Wrong mental model:

The full asynchronous chain has definitely finished.

Correct mental model:

The request was accepted and the producer path was initiated; downstream completion is separate.

RouteX example:

The controller returns before proving matching and notification already finished.

## 23. Production reality

RouteX is a learning architecture, not a production-ready dispatch system.

It currently simplifies or omits:

| Concern | Why it matters in real systems |
| --- | --- |
| driver availability persistence | real assignment needs durable and current driver state |
| retries | transient failures happen in production |
| DLT/DLQ | poison messages need controlled handling |
| idempotency | duplicate delivery or reprocessing can happen |
| transactions | some workflows need stronger atomicity guarantees |
| acknowledgements strategy | delivery and retry behavior depend on ack/commit semantics |
| observability | production systems need metrics, tracing, structured logs, and lag monitoring |
| schema evolution | event contracts change over time across independent deployments |
| replication | single-broker storage is not resilient enough for production |
| security | real clusters need authn/authz and secure transport |
| multi-broker deployment | production Kafka is normally clustered |
| external notification infrastructure | console printing is not SMS/email delivery |
| exactly-once discussions | strong semantics require careful end-to-end design, not assumptions |

Do not describe the current app as production-ready.

## 24. Interview revision

### What is Kafka?

A distributed event-streaming platform that stores records durably in topic partitions and lets producers publish and consumers read asynchronously.

### What is a broker?

A Kafka server process that receives, stores, and serves records.

### What is a topic?

A named stream of records, internally split into partitions.

### What is a partition?

An append-only ordered log segment within a topic.

### Why partitions?

To scale storage and throughput, enable parallel consumption, and preserve ordering within each partition.

### What is a consumer group?

A logical subscription identity shared by one or more consumer instances so Kafka can track progress and distribute partitions.

### Consumer vs consumer group?

A consumer is one running client instance; a consumer group is the logical workload-sharing identity.

### What happens with 3 partitions and 5 consumers?

In one group, at most 3 consumers can actively own those partitions for that topic; the others may be idle.

### What happens with 10 partitions and 3 consumers?

All 3 can be active, and some consumers will own multiple partitions.

### What is an offset?

A partition-relative position of a record in the log.

### Where are committed consumer offsets tracked?

Kafka tracks committed consumer-group offsets internally, conceptually per group/topic/partition, using Kafka-managed internal storage such as `__consumer_offsets`.

### What is `KafkaTemplate`?

Spring Kafka’s producer abstraction over the Kafka Java producer client.

### What does `@KafkaListener` actually do?

It marks a method for Spring Kafka to wire into a listener container that manages a Kafka consumer, polls Kafka, deserializes records, and invokes the method.

### What is serialization?

Converting an in-memory object into bytes for transport and storage.

### Why does Kafka store bytes?

Because brokers operate on transport/storage formats, not Java object memory.

### What is event chaining?

One consumed event triggers business logic that publishes a new event representing a new completed fact.

### Why not call notification directly?

Because Kafka topic boundaries keep matching and notification loosely coupled and independently evolvable.

### What is a Kafka message key?

Metadata attached to a record that can influence partition selection and help keep related records together.

### What ordering does Kafka guarantee?

Ordering within a partition, not global ordering across a multi-partition topic.

### What is rebalancing?

Reassignment of partition ownership within a consumer group when membership or relevant subscription state changes.

### What happens if a consumer crashes?

Kafka can eventually reassign that group’s partitions to other active consumers in the group.

### Can a producer publish when no consumer exists?

Yes. Kafka can store the record even if no consumer is currently running.

### Does consuming delete a Kafka record?

No. Consumption advances consumer progress; retention and deletion are separate concerns.

### What is `__consumer_offsets`?

Kafka’s internal mechanism/topic for tracking committed consumer-group offset metadata.

### Why can two different consumer groups both process the same record?

Because Kafka tracks progress independently per consumer group, so one group’s progress does not erase another group’s ability to read the record.

## 25. Final cheat sheets

### RouteX flow

```text
HTTP
-> Controller
-> Event
-> KafkaTemplate
-> KafkaProducer
-> Serializer
-> Broker
-> Topic / Partition
-> KafkaConsumer.poll()
-> Deserializer
-> Listener
-> Business logic
-> New event
-> Kafka again
-> Downstream consumer
```

### Consumer group rules

```text
Same group
-> workload sharing

Different groups
-> independent logical consumption

Active consumers for a topic in one group
<= number of assigned partitions
```

### One-sentence phase summaries

| Phase | Summary |
| --- | --- |
| Phase 1 | Learn how one ride-request event moves from HTTP into Kafka and back out into asynchronous processing. |
| Phase 2 | Learn how one consumed event can create a second event and form an asynchronous business workflow. |
| Phase 3 | Learn how partitions, consumer groups, and offsets shape distribution, progress tracking, and visibility of Kafka consumption. |

## 26. Current repository inconsistencies corrected from the older README

The previous README no longer matched the repository in several places. The current document corrects those mismatches:

- the code uses `DriverAssignmentEvent`, not `DriverAssignedEvent`
- the notification consumer now exists and is implemented
- the old README described a no-consumer intermediate phase as if it were current
- the current package structure is `dispatch`, `matching`, and `notification`, not the planned `assignment.*` package layout described earlier
- the effective listener group IDs are `driver-matching-group` and `notification-group`
- both topics are currently configured with `3` partitions
- the listeners currently log partition metadata
- `spring.json.trusted.packages` currently includes both `com.routex.dispatch` and `com.routex.matching`
- Kafka host/UI ports are `9092` and `8081`
- the Compose file now uses `CLUSTER_ID`, not the earlier broken variant

If README and source ever disagree again, source and current configuration should win.

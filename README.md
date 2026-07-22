# RouteX — Phase 1: one ride request, one event

This milestone implements a REST endpoint that accepts a ride request, publishes a `RideRequestedEvent` to Kafka, and has a separate Kafka consumer log that event.

## The real-world problem

When a passenger asks for a ride, many independently evolving parts of a dispatch platform may care: matching, price calculation, notifications, fraud checks, and analytics. Calling all of them directly from the HTTP request makes the request slow and tightly couples the ride API to every downstream concern. If a downstream service is down, the passenger-facing API is also affected.

Kafka lets the ride API record one fact — “a ride was requested” — in a shared event log. Other services can independently react to that fact. Phase 1 has only one consumer that prints the event, so we can observe this flow before adding real dispatch behavior.

## Vocabulary, from first principles

| Term | Meaning in RouteX |
| --- | --- |
| **Event** | An immutable record that something happened. `RideRequestedEvent` says a specific passenger requested a ride at a particular time. |
| **Producer** | A program that writes an event to Kafka. `RideRequestController` is the producer. |
| **Consumer** | A program that reads events from Kafka. `RideRequestedEventConsumer` is the consumer. |
| **Broker** | The Kafka server process that receives, stores, and serves events. Docker starts one broker. |
| **Topic** | A named category of events held by Kafka. `ride-requested` holds ride-request events. |
| **Serialization** | Turning an in-memory Java object into bytes for network/storage transfer. The producer uses JSON serialization. Deserialization reverses that process for the consumer. |
| **Kafka UI** | A browser application that connects to Kafka and lets us inspect topics and their records. It is not Kafka itself. |

## Kafka architecture and internal path

```text
HTTP client
    | POST /rides/requests
    v
RouteX REST controller (producer)
    | Java record -> JSON bytes
    v
Kafka broker, topic: ride-requested
    | stores the bytes durably
    v
RouteX listener (consumer)
    | JSON bytes -> Java record
    v
application log
```

1. The controller creates an immutable event with a new `rideId` and timestamp.
2. `KafkaTemplate.send(...)` gives Spring Kafka the topic name, a key (`rideId`), and the event object.
3. The configured JSON serializer converts the event object to JSON bytes; Kafka transports and stores bytes, not Java objects.
4. The producer client sends those bytes to the broker at `localhost:9092`.
5. The broker stores the record in `ride-requested`.
6. Spring Kafka’s listener container continuously polls Kafka on behalf of the annotated consumer.
7. The JSON deserializer turns the stored bytes back into `RideRequestedEvent`, then Spring invokes `logRideRequest`.

Kafka is designed as a durable shared log rather than a direct function call: producers need not know consumer addresses, consumers can run at their own pace, and stored events can be inspected later. This is the foundation for independently deployable services.

We will intentionally defer how Kafka scales a topic, tracks each consumer’s progress, retries failures, and provides stronger delivery guarantees. Those are later milestones; do not treat this small demo as production dispatch semantics yet.

## Spring Kafka mapping

| Spring application concept | Kafka/client responsibility |
| --- | --- |
| `KafkaTemplate` | Wraps Kafka’s Java producer client and sends a record. |
| `JsonSerializer` | Converts the event to bytes before the producer sends it. |
| `NewTopic` bean | Spring Boot’s Kafka admin support asks the broker to create the named topic at startup. |
| `@KafkaListener` | Spring creates a listener container around Kafka’s Java consumer client and calls the annotated method for each deserialized event. |
| `JsonDeserializer` | Converts the bytes read by the consumer back to `RideRequestedEvent`. |

## Code walkthrough

### `RouteXApplication`

- `package com.routex;` declares the root package. Spring scans this package and its child packages for components.
- The two imports make Spring Boot’s launcher and annotation available.
- `@SpringBootApplication` combines Spring configuration, auto-configuration, and component scanning. It causes Boot to build web and Kafka infrastructure from the dependencies and `application.yaml`.
- `main` is Java’s entry point.
- `SpringApplication.run(...)` builds the Spring application context, starts embedded HTTP serving, creates Kafka infrastructure, and starts listener containers.

### Event and HTTP request records

- `RideRequestedEvent` is a Java `record`: each listed component becomes a final field, accessor, constructor component, `equals`, `hashCode`, and `toString`.
- `rideId` identifies this event’s ride; `passengerId`, `pickupLocation`, and `destinationLocation` describe it; `requestedAt` records when RouteX accepted it.
- `RideRequest` is deliberately separate: it is the mutable boundary shape supplied by an HTTP caller. It does not let callers choose an internal ride ID or timestamp.
- `@NotBlank` says a field must contain non-whitespace text. Spring validates it before invoking the controller method.

### `KafkaTopicConfiguration`

- `@Configuration` marks this class as a source of Spring bean definitions.
- `RIDE_REQUESTED_TOPIC` keeps the topic name in one place so producer and consumer cannot silently drift.
- `@Bean` registers the returned `NewTopic` as a Spring-managed object.
- `TopicBuilder.name(...)` specifies the topic to create. We deliberately leave broker defaults in place for this first single-broker lesson.
- Boot finds the `NewTopic` bean and uses an admin client to request creation from the broker. Re-running startup is safe: Kafka reports the topic already exists.

### `RideRequestController`

- `@RestController` makes this class handle HTTP requests and serialize returned values as JSON.
- `@RequestMapping("/rides")` supplies the shared URL prefix.
- `KafkaTemplate<String, RideRequestedEvent>` means this template uses string keys and ride-request event values.
- Constructor injection makes the template an explicit required dependency and lets Spring supply its configured instance.
- `@PostMapping("/requests")` maps `POST /rides/requests`.
- `@RequestBody` converts request JSON into `RideRequest`; `@Valid` applies its `@NotBlank` constraints first.
- The `new RideRequestedEvent(...)` block creates the fact RouteX will publish. `UUID.randomUUID()` generates the ride ID and `Instant.now()` records acceptance time.
- `send(topic, event.rideId(), event)` sends the key and event. Spring’s producer serializes both then sends them to the broker. This method is asynchronous; Phase 1 returns `202 Accepted` after handing the send to the producer client. We will study send acknowledgement and failures later.
- `ResponseEntity.status(HttpStatus.ACCEPTED)` returns the created event to the HTTP caller with status 202.

### `RideRequestedEventConsumer`

- `@Component` allows component scanning to register this class.
- `@KafkaListener` tells Spring Kafka to create and start a Kafka listener container for `ride-requested`.
- `logRideRequest(RideRequestedEvent event)` is invoked after the consumer receives bytes and the configured JSON deserializer creates the event object.
- `System.out.printf` is intentionally plain for visibility. In a real service this would call a focused application service and use structured logging.

## Configuration walkthrough

### `application.yaml`

| Property | Meaning |
| --- | --- |
| `spring.application.name` | The Spring application’s local name; useful in logs and later observability. |
| `spring.kafka.bootstrap-servers` | The initial broker address the Kafka client contacts to learn cluster metadata. `localhost:9092` is the Docker port exposed to your host. |
| `spring.kafka.admin.fail-fast` | Fail RouteX startup if the admin client cannot reach Kafka, rather than appearing healthy with no broker. |
| `producer.key-serializer` | Converts the `rideId` string key to bytes. |
| `producer.value-serializer` | Converts `RideRequestedEvent` to JSON bytes. |
| `consumer.group-id` | A required consumer subscription identity. It is configured now solely so the listener can subscribe; consumer-group behavior is a later lesson. |
| `consumer.auto-offset-reset` | If this fresh identity has no remembered reading position, start with the earliest stored records. The meaning of reading positions is deferred to the offsets milestone. |
| `consumer.key-deserializer` | Converts a record key’s bytes to `String`. |
| `consumer.value-deserializer` | Converts JSON bytes to the event type indicated by Spring Kafka’s type metadata. |
| `consumer.properties.spring.json.trusted.packages` | Security allow-list: permit JSON type metadata from our event package only, never arbitrary classes. |

### `compose.yaml`

The `kafka` service runs one Kafka broker locally. The `KAFKA_*` values establish its single-node local network setup; `KAFKA_ADVERTISED_LISTENERS` is especially important because it tells host applications to use `localhost:9092` while Docker containers use `kafka:29092`. `kafka-ui` connects over the Docker network and is published at port 8081.

`KAFKA_PROCESS_ROLES=broker,controller` runs both local roles in one process. In this early local environment, that minimizes moving pieces; it is not a production topology. The controller-related settings are Kafka’s current internal metadata-management setup and are intentionally not a Phase 1 study topic.

## Run the milestone

1. Start Kafka and the UI:

   ```powershell
   docker compose up -d
   ```

2. Open Kafka UI at `http://localhost:8081`.

3. Install Maven or open the `pom.xml` as a Maven project in IntelliJ, then start RouteX:

   ```powershell
   mvn spring-boot:run
   ```

   On first startup the application creates `ride-requested`.

4. Publish an event:

   ```powershell
   Invoke-RestMethod -Method Post -Uri http://localhost:8080/rides/requests -ContentType 'application/json' -Body '{"passengerId":"passenger-42","pickupLocation":"Koramangala","destinationLocation":"Indiranagar"}'
   ```

5. Observe the `Ride request received: ...` line in the RouteX console. In Kafka UI, open Topics → `ride-requested` → Messages and inspect the stored JSON.

6. Stop the infrastructure when finished:

   ```powershell
   docker compose down
   ```

## Experiments

1. Send three requests, then inspect the three stored records in Kafka UI. Notice that the endpoint and the consumer have no direct HTTP call between them.
2. Stop RouteX, send nothing, restart it, then submit a request and observe the same pipeline. Kafka outlives the application process.
3. Stop RouteX, submit a request (the HTTP call will fail because RouteX is the producer), then start RouteX and submit again. Contrast “the broker is available” with “the producer application is available.”
4. Change `pickupLocation` to a blank string. Observe the HTTP validation error and confirm no event reaches Kafka.
5. In Kafka UI, inspect the message value. Compare its JSON fields with `RideRequestedEvent` and identify what the serializer had to preserve.

## Check your understanding

1. Why is publishing an event a better boundary than having the ride API call an analytics service directly?
2. What is the difference between the Kafka broker and Kafka UI?
3. Why must an event be serialized before Kafka can store it?
4. Which code is the producer, which code is the consumer, and what is the topic’s name?
5. If the controller can return an HTTP response without the consumer method being called immediately, what does that tell you about their coupling?

Reply with your answers before we move to the next phase. In particular, explain the producer-to-broker-to-consumer path in your own words.

---

# Phase 2 - Event Chaining

> Implementation rule for this phase: you write the Java code. This document specifies the design, class responsibilities, and verification steps, but deliberately contains no implementation code.

## Goal

Turn the Phase 1 single-event example into an event-driven pipeline. A ride request should lead to an assignment event, which should lead to a passenger notification.

```text
HTTP request
  -> RideRequestController
  -> RideRequestedEvent
  -> Kafka topic: ride-requested
  -> RideRequestedConsumer
  -> DriverAssignmentService
  -> DriverAssignedEvent
  -> Kafka topic: driver-assigned
  -> NotificationConsumer
  -> console notification
```

## Problem

Logging a ride request does not create any business outcome. A dispatch platform must assign a driver, then notify the passenger. Putting both responsibilities into the HTTP controller, or into one large Kafka listener, couples unrelated work together.

Event chaining solves this by making each completed business fact the input to the next focused responsibility. The assignment stage reacts to a ride request. The notification stage reacts to a completed assignment. Neither stage needs a direct Java or HTTP call to the other.

## Theory: events, commands, and an event chain

An **event** is an immutable statement that something has already happened. `RideRequestedEvent` means a ride request was accepted. `DriverAssignedEvent` means RouteX has assigned a driver.

A **command** asks for work to happen. For example, `AssignDriverCommand` would request an assignment. We are not introducing commands in this phase; the assignment listener reacts to the completed fact represented by `RideRequestedEvent` and publishes the completed outcome represented by `DriverAssignedEvent`.

An **event chain** is a sequence where a component consumes a completed fact, performs its own responsibility, and publishes another completed fact. It is not a chain of direct method calls between business capabilities.

## Kafka internals

1. The broker persists a `RideRequestedEvent` record in `ride-requested`.
2. Spring Kafka's listener container obtains that record and deserializes its JSON bytes into `RideRequestedEvent`.
3. `RideRequestedConsumer` passes the event to assignment logic.
4. The assignment logic creates a `DriverAssignedEvent`.
5. The consumer uses a Kafka producer to serialize and send that new event to `driver-assigned`.
6. Kafka persists the new record independently from the original record.
7. Spring Kafka's notification listener reads and deserializes the `driver-assigned` record.
8. `NotificationConsumer` prints the notification.

Kafka does not invoke the notification consumer from the assignment consumer. Kafka stores records under topic names; consumers independently read the topics they subscribe to. This is why the assignment capability does not need the notification capability's network address, Java type, or deployment schedule.

## Why this architecture exists

The assignment capability should decide who drives. The notification capability should communicate the outcome. Those are separate reasons to change, so they should be separate components.

This creates a clean extension point: an Analytics service could later consume `driver-assigned` without changing assignment or notification code. This pattern is common in large event-driven systems where matching, pricing, fraud checks, customer communication, and analytics are owned and deployed independently.

For this local application all components run in one Spring Boot process. The Kafka topic boundary is still valuable because it models the same decoupled contract that would exist if these capabilities became separate services.

## Spring Kafka mapping

| Application responsibility | Spring/Kafka mechanism |
| --- | --- |
| Read `ride-requested` events | `@KafkaListener` on `RideRequestedConsumer` |
| Select a driver | A regular Spring `@Service` |
| Send `DriverAssignedEvent` | `KafkaTemplate` |
| Store assignment events | Kafka topic `driver-assigned` |
| Read assignment events | `@KafkaListener` on `NotificationConsumer` |
| Create the topic at startup | `NewTopic` bean in topic configuration |

`@KafkaListener` causes Spring Kafka to create a listener container around Kafka's Java consumer client. It polls Kafka in the background and invokes the annotated method after deserialization.

`KafkaTemplate` wraps Kafka's Java producer client. When you send a `DriverAssignedEvent`, the configured JSON serializer turns it into bytes and the producer sends those bytes to the broker.

## Package plan

Keep existing Phase 1 files in `com.routex.dispatch`. Create the following packages:

```text
com.routex.assignment
com.routex.assignment.event
com.routex.assignment.messaging
com.routex.assignment.service
com.routex.notification
com.routex.notification.messaging
```

Responsibilities:

| Package | Owns |
| --- | --- |
| `assignment.event` | Assignment-related event contracts. |
| `assignment.messaging` | Kafka-facing adapters for assignment. |
| `assignment.service` | Driver-selection business logic. |
| `notification.messaging` | Kafka-facing adapters for notifications. |

Keep the topic names in the existing topic configuration class for this phase. Do not duplicate topic-name strings across producer and consumer classes.

## Classes you should create

### `DriverAssignedEvent`

Package: `com.routex.assignment.event`

Create an immutable Java record representing a completed assignment. Include:

- `rideId`
- `passengerId`
- `driverId`
- `assignedAt`

`rideId` ties the assignment to its originating ride request. Include `passengerId` because the notification consumer needs enough data to notify the passenger without reconstructing it elsewhere. `driverId` is the assignment outcome. `assignedAt` captures when that outcome was produced.

### `DriverAssignmentService`

Package: `com.routex.assignment.service`

Annotate this class with `@Service`. This registers it as a Spring bean and communicates that it owns application/business behavior.

Give it one public method that accepts a `RideRequestedEvent` and returns a `DriverAssignedEvent`.

Inside the method:

1. Accept the requested-ride fact.
2. Select a deterministic placeholder driver ID, such as `driver-demo-101`.
3. Carry the original ride ID and passenger ID into a new assignment event.
4. Set the assignment timestamp.
5. Return the event.

Do not put Kafka publishing in this service in Phase 2. Its job is to make an assignment decision, not to know messaging infrastructure.

### `RideRequestedConsumer`

Package: `com.routex.assignment.messaging`

Register this class as a Spring component. It needs two dependencies: `DriverAssignmentService` and a `KafkaTemplate` that can send `DriverAssignedEvent` values.

Create one listener method annotated with `@KafkaListener` for `ride-requested`.

Inside the listener method:

1. Receive the deserialized `RideRequestedEvent`.
2. Delegate to `DriverAssignmentService`.
3. Receive the resulting `DriverAssignedEvent`.
4. Send it to `driver-assigned` using the original `rideId` as the message key.

This class adapts Kafka input to the assignment use case and publishes its outcome. It must not contain driver-selection rules.

### `NotificationConsumer`

Package: `com.routex.notification.messaging`

Register this class as a Spring component.

Create one listener method annotated with `@KafkaListener` for `driver-assigned`.

Inside the method:

1. Receive the deserialized `DriverAssignedEvent`.
2. Build a clear console message identifying the passenger, ride, and assigned driver.
3. Print the message.

Console output is deliberately the entire notification implementation for this phase. Do not build push, email, or SMS infrastructure yet.

### Update `KafkaTopicConfiguration`

Add a constant for `driver-assigned` and add a second `NewTopic` bean for it.

`@Configuration` identifies the class as a source of bean definitions. `@Bean` registers the returned `NewTopic` object with Spring; Spring Boot's Kafka admin support then asks the broker to create the topic at application startup.

## Configuration task

Both new event types remain below `com.routex`, so retain a narrow JSON trusted-package allow-list such as `com.routex`. Do not use a wildcard trusted package just to make deserialization work.

Configure distinct consumer listener identities for the assignment listener and notification listener. They must not share the same identity. We will study the consumer-group mechanics underlying those identities in a later phase; for now use purpose-revealing values, one for assignment and one for notification.

## Annotation reference

| Annotation | Where to use it | Why |
| --- | --- | --- |
| `@Service` | `DriverAssignmentService` | Registers business logic as a Spring bean and communicates its responsibility. |
| `@Component` | Kafka consumer classes | Registers the class so Spring can discover listener methods. |
| `@KafkaListener` | Each listener method | Starts a Spring Kafka listener container for the specified topic. |
| `@Configuration` | Existing topic configuration | Declares a source of Spring bean definitions. |
| `@Bean` | Each topic factory method | Registers a `NewTopic` for Kafka-admin topic creation. |

## Common beginner mistakes

- Putting assignment rules directly inside `RideRequestedConsumer` instead of `DriverAssignmentService`.
- Publishing `DriverAssignedEvent` from `RideRequestController`, which bypasses the event-driven pipeline.
- Reusing `RideRequestedEvent` on the `driver-assigned` topic. A topic should contain the fact its name promises.
- Omitting `rideId` from the assignment event and losing the business link between request and assignment.
- Hardcoding `driver-assigned` in several classes.
- Calling assignment logic from the notification consumer.
- Letting the HTTP caller provide the driver ID. Driver assignment is server-owned business logic.
- Adding retries, transactions, or advanced consumer configuration before understanding the basic chain.

## Production perspective

The fixed demo driver is intentional. A production assignment service would need nearby-driver search, availability data, eligibility checks, concurrency protection, an assignment policy, observability, and clear failure handling.

Production event contracts also need careful evolution: consumers may be deployed at different times, so avoid casually removing or renaming event fields. You will study resilient delivery, failure handling, and duplicate-safe business behavior after the basic event chain is solid.

## Implementation checklist

1. Add the `driver-assigned` topic to the topic configuration.
2. Create `DriverAssignedEvent` as an immutable record.
3. Create `DriverAssignmentService` with a fixed demo driver assignment.
4. Create `RideRequestedConsumer` to consume, delegate, and publish.
5. Create `NotificationConsumer` to consume and log.
6. Configure separate listener identities for assignment and notification.
7. Start Kafka and RouteX.
8. Submit one HTTP ride request.
9. Verify `ride-requested` contains the request in Kafka UI.
10. Verify `driver-assigned` contains the assignment in Kafka UI.
11. Verify the RouteX console prints the notification after the assignment event is consumed.

## Questions before the next milestone

### Backend interview questions

1. Why should `DriverAssignmentService` return a `DriverAssignedEvent` instead of publishing directly to Kafka?
2. Why publish `DriverAssignedEvent` to a separate topic rather than call notification code from the assignment consumer?
3. What business fact does `DriverAssignedEvent` represent, and why should it be immutable?

### Production debugging scenarios

1. Kafka UI shows new `ride-requested` records but `driver-assigned` remains empty. Which component boundaries would you inspect first, and what evidence would you seek?
2. Kafka UI shows `driver-assigned` records but no notification appears in the console. How would you distinguish a notification-listener problem from a deserialization problem?

### Architectural design question

An Analytics team wants a real-time count of assignments. Would you modify `RideRequestedConsumer`, modify `NotificationConsumer`, or add a new consumer of `driver-assigned`? Explain the coupling consequences.

Do not proceed to the next milestone until you can answer these questions and have had your implementation reviewed.

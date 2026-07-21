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

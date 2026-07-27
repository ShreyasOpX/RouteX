# Kafka Foundations in RouteX

## RouteX Mapping

Kafka lets the HTTP API publish the fact that a ride was requested without calling matching or notification directly. A producer writes a record to a broker; consumers later read that stored record at their own pace.

```text
HTTP request -> RideRequestController -> KafkaTemplate -> Kafka broker
             -> topic/partition -> Spring listener container -> @KafkaListener method
```

| Kafka concept | RouteX implementation |
| --- | --- |
| Broker | One local KRaft Kafka broker in `compose.yaml` |
| Bootstrap server | `localhost:9092` in `application.yaml` |
| Topics | `ride-requested`, `driver-assigned`, `ride-requested-dlt` |
| Producer abstraction | `KafkaTemplate` |
| Consumers | `DriverMatchingConsumer`, `DriverAssignmentNotificationConsumer`, `DeadLetterConsumer` |
| Event values | Java records serialized as JSON |
| Record key | `rideId` serialized as a string |
| Listener abstraction | `@KafkaListener` and Spring Kafka listener containers |

Kafka stores bytes, not Java objects. RouteX configures `StringSerializer`/`StringDeserializer` for keys and Spring Kafka JSON serializer/deserializer for values. The JSON trusted-package allow-list is `com.routex.dispatch,com.routex.matching`.

`KafkaTopicConfiguration` supplies `NewTopic` beans. `spring.kafka.admin.fail-fast: true` means RouteX fails startup if its admin client cannot reach Kafka. Kafka UI connects from Docker at `kafka:29092` and is exposed on `http://localhost:8081`.

## Practical Verification

### Prerequisites

```powershell
docker compose up -d
mvn spring-boot:run
```

### Trigger

```powershell
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8080/rides/requests" `
    -ContentType "application/json" `
    -Body '{"passengerId":"passenger-101","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
```

### Expected Output

The HTTP response contains the `RideRequestedEvent` fields, including `rideId=<generated-uuid>`. RouteX then prints lines in this actual format:

```text
MATCHING | ride=<generated-uuid> | partition=<0|1|2> | offset=<dynamic-offset>
NOTIFICATION: Passenger passenger-101 - Driver <Arjun|Rahul|Kiran> (<vehicle-number>) has been assigned to ride <generated-uuid>
MATCHING | ride=<generated-uuid> | partition=<0|1|2>
```

### What This Demonstrates

The controller produced JSON to Kafka; Spring deserialized it for matching; matching produced a second event; notification consumed that second event. The matching line is emitted before driver selection and the notification line is emitted only after `driver-assigned` is consumed.

## Key Takeaways

- A Kafka producer writes a record to a broker, not directly to a consumer method.
- A record has a topic, partition, key, value, headers, and an offset once stored.
- Spring Boot configures Kafka clients from `application.yaml`; Spring Kafka supplies `KafkaTemplate` and listener containers.

# `driver-assigned` Topic

## Purpose and Contract

`driver-assigned` carries the matching outcome. `DriverAssignmentService` randomly selects one of the in-memory demo drivers (`D101`, `D102`, or `D103`) and returns a `DriverAssignmentEvent`; it has no Kafka dependency. `DriverAssignmentProducer.publish` sends that event with `rideId` as the key.

| Event field | Meaning |
| --- | --- |
| `rideId`, `passengerId` | Correlate assignment to the request |
| `driverId`, `driverName`, `vehicleNumber` | Selected demo driver details |
| `assignedAt` | Assignment `Instant` |

The topic has 3 declared partitions. `DriverAssignmentNotificationConsumer.handleDriverAssigned` consumes it in `notification-group`, prints the assignment, then manually acknowledges it. It is console notification only; there is no email, SMS, or push integration.

## Practical Verification

### Prerequisites

Start Docker Kafka and RouteX as shown in the [README](../README.md#run-locally).

### Trigger

```powershell
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8080/rides/requests" `
    -ContentType "application/json" `
    -Body '{"passengerId":"passenger-101","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
```

### Expected Output

After the matching line, the notification consumer prints:

```text
NOTIFICATION: Passenger passenger-101 - Driver <Arjun|Rahul|Kiran> (<vehicle-number>) has been assigned to ride <generated-uuid>
MATCHING | ride=<generated-uuid> | partition=<0|1|2>
```

The final line is literally labelled `MATCHING` in the current notification source, even though the notification consumer emits it.

### What This Demonstrates

`DriverMatchingConsumer` did not call notification directly. It published a new fact to `driver-assigned`, then `notification-group` independently consumed that topic. The same ride ID lets the two topic records be correlated.

## Key Takeaways

- A second topic creates a durable boundary between matching and notification.
- The topic value is `DriverAssignmentEvent`; its key is again `rideId`.
- Manual acknowledgement marks the successful notification-listener path in application code.

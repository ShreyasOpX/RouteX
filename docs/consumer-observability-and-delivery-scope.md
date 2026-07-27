# Consumer Observability

## What RouteX Actually Logs

| Component | Actual source output | Metadata exposed |
| --- | --- | --- |
| `DriverMatchingConsumer` | `MATCHING | ride=%s | partition=%d | offset=%d` | ride ID, received partition, received offset |
| `DriverAssignmentNotificationConsumer` | notification sentence, then `MATCHING | ride=%s | partition=%d` | passenger, driver, vehicle, ride ID, partition |
| `DeadLetterConsumer` | `DLT | ride=%s | passenger=%s | partition=%d | offset=%d` plus one `DLT HEADER` line per header | DLT record value, DLT partition/offset, every header key/value |

The notification consumer's second line is literally labelled `MATCHING` in source despite being notification output. No active source log includes topic name, consumer thread, consumer/client ID, group ID, timestamp, rebalance event, retry-attempt number, or metrics/tracing data.

Partition and offset reveal where a matching record lives. A repeated matching line with the same partition/offset during `failure-test` is the observable sign that the same source record is being re-delivered; the code itself does not print an attempt counter.

## Practical Verification

### Prerequisites

Start Docker Kafka and RouteX as shown in the [README](../README.md#run-locally).

### Trigger

```powershell
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8080/rides/requests" `
    -ContentType "application/json" `
    -Body '{"passengerId":"failure-test","pickupLocation":"MSRIT","destinationLocation":"Electronic City"}'
```

### Expected Output

The application prints the matching format four times (one initial delivery plus three retries):

```text
MATCHING | ride=<generated-uuid> | partition=<0|1|2> | offset=<same-dynamic-offset>
...
MATCHING | ride=<generated-uuid> | partition=<same-partition> | offset=<same-dynamic-offset>
```

Spring's framework logger may also print the thrown `Simulated driver matching failure`; its full format is not defined by RouteX source. No `NOTIFICATION` line is expected.

### What This Demonstrates

The source logs enough record metadata to correlate retries with the same Kafka record. It does not identify individual consumer instances, so thread-level concurrency cannot be inferred from these lines.

## Key Takeaways

- Partition plus offset is a useful record identity inside a topic.
- Logs must be interpreted according to their literal source; the notification partition line is mislabelled.
- RouteX does not currently provide structured logging, metrics, tracing, or retry-attempt log fields.

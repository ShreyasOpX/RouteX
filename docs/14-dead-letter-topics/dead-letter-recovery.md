# Dead-Letter Recovery

## Overview

RouteX recovers exhausted `ride-requested` failures to `ride-requested-dlt`.

## Why this exists

It preserves a record that could not complete after the configured retry policy.

## Problem it solves

The failing source partition can progress after recovery while failure context remains inspectable.

## RouteX implementation

`DeadLetterPublishingRecoverer` has a resolver that returns `new TopicPartition(RIDE_REQUESTED_DLT, record.partition())`; `DeadLetterConsumer` monitors that topic.

## Code walkthrough

Read `KafkaErrorHandlerConfiguration.java`, `KafkaTopicConfiguration.java`, and `reliability/DeadLetterConsumer.java`.

## Execution flow

```mermaid
flowchart LR
 S[ride-requested Pn] --> F[retries exhausted]
 F --> R[recoverer]
 R --> D[ride-requested-dlt Pn]
 D --> M[DLT monitor]
```

## Kafka internals

The recovered record is a new Kafka record in the DLT. Spring Kafka adds failure/original-record headers; the DLT consumer prints received header bytes as strings.

## Spring Boot internals

`DefaultErrorHandler` invokes its recoverer when backoff attempts are exhausted.

## Observe this in RouteX

Send `failure-test`, wait for retries, then inspect `ride-requested-dlt` in Kafka UI and `DLT`/`DLT HEADER` console output.

## Production implementation

| RouteX | Production |
| --- | --- |
| DLT monitor prints records | Alerting, triage, replay tooling, and ownership workflow |
| Header bytes rendered as strings | Typed decoding/redaction of exception and source metadata |

## Trade-offs

DLTs protect forward progress but require an operational decision about replay, discard, or repair.

## Common mistakes

- Treating a DLT as automatic successful processing.
- Ignoring original topic/partition/offset metadata.

## Best practices

Assign DLT ownership, retain context, and make replay idempotent.

## Interview questions

**Why preserve the source partition?** RouteX routes recovery to the same partition number for a clear relationship to the source record.

**Follow-up:** Is a DLT record the same physical record? No; it is a newly produced record.

**Enterprise discussion:** A DLT is an operations workflow, not merely a topic.

## Quick revision

Exhausted `ride-requested Pn` → recoverer → `ride-requested-dlt Pn` → monitor consumer.


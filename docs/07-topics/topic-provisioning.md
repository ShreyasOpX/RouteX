# Topic Provisioning in RouteX

## Overview

RouteX declares its application topics as Spring `NewTopic` beans.

## Why this exists

Topology is part of the application contract, not an invisible prerequisite.

## Problem it solves

It makes local topics available at application startup through Spring Kafka administration.

## RouteX implementation

`KafkaTopicConfiguration` declares `ride-requested`, `driver-assigned`, and `ride-requested-dlt`, each with three partitions.

## Code walkthrough

Read `dispatch/KafkaTopicConfiguration.java`; `application.yaml` sets `spring.kafka.admin.fail-fast: true`.

## Execution flow

```mermaid
flowchart LR
 B[NewTopic beans] --> A[KafkaAdmin] --> K[Kafka metadata] --> T[topics]
```

## Kafka internals

A topic is partition metadata plus partition logs. Topic creation is a cluster metadata operation.

## Spring Boot internals

Boot auto-configures Kafka administration; Spring Kafka uses `NewTopic` definitions during context initialization.

## Observe this in RouteX

Start Kafka and RouteX, then inspect the three application topics in Kafka UI.

## Production implementation

| RouteX | Production |
| --- | --- |
| Application-created local topics | Infrastructure-controlled topic lifecycle and policy |
| Partition-only declarations | Explicit replication, retention, cleanup, and ACL policy |

## Trade-offs

Application provisioning is convenient locally; centralized governance reduces production drift.

## Common mistakes

- Assuming topic names are type-safe outside constants.
- Changing partition count without considering key distribution and ordering history.

## Best practices

Treat names, partitions, retention, replication, and ownership as versioned topology decisions.

## Interview questions

**Why use `NewTopic` in RouteX?** It makes local topology reproducible.

**Follow-up:** Does three partitions mean three consumers always work? No; group membership controls assignment.

**Enterprise discussion:** Production topic changes need ownership and compatibility review.

## Quick revision

`KafkaTopicConfiguration` is RouteX's topology source: three named, three-partition topics.

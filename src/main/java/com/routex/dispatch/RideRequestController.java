package com.routex.dispatch;

import jakarta.validation.Valid;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;
import org.springframework.web.bind.annotation.PathVariable;

@RestController
@RequestMapping("/rides")
public class RideRequestController {

    private final KafkaTemplate<String, RideRequestedEvent> kafkaTemplate;

    public RideRequestController(KafkaTemplate<String, RideRequestedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/requests")
    public ResponseEntity<RideRequestedEvent> requestRide(@Valid @RequestBody RideRequest request) {
        RideRequestedEvent event = new RideRequestedEvent(
                UUID.randomUUID().toString(),
                request.passengerId(),
                request.pickupLocation(),
                request.destinationLocation(),
                Instant.now());

        // kafkaTemplate.send(KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC,
        // event.rideId(), event);
        CompletableFuture<SendResult<String, RideRequestedEvent>> future = kafkaTemplate
                .send(KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC, event.rideId(), event);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.printf(
                        "PRODUCER ERROR | topic=%s | key=%s | error=%s%n",
                        KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC,
                        event.rideId(),
                        ex.getMessage());
                return;
            }
            var metadata = result.getRecordMetadata();
            System.out.printf(
                    "PRODUCER ACK | topic=%s | partition=%d | offset=%d | key=%s%n",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    event.rideId());
        });

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(event);
    }

    @PostMapping("/bulk/{count}")
    public ResponseEntity<String> bulkPublish(@PathVariable int count) {
        for (int i = 0; i < count; i++) {

            RideRequestedEvent event = new RideRequestedEvent(
                    UUID.randomUUID().toString(),
                    "P" + i,
                    "MSRIT",
                    "AIRPORT",
                    Instant.now());

            kafkaTemplate.send(
                    KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC,
                    event.rideId(),
                    event);
        }

        return ResponseEntity.ok("Published " + count + " ride requests.");
    }
}

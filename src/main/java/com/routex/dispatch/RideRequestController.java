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
                Instant.now()
        );

        kafkaTemplate.send(KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC, event.rideId(), event);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(event);
    }
}


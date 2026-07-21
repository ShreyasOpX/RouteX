package com.routex.dispatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RideRequestedEventConsumer {

    @KafkaListener(topics = KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC)
    public void logRideRequest(RideRequestedEvent event) {
        System.out.printf("Ride request received: %s%n", event);
    }
}


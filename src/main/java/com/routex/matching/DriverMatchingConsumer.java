package com.routex.matching;

import com.routex.dispatch.KafkaTopicConfiguration;
import com.routex.dispatch.RideRequestedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DriverMatchingConsumer {
    private final DriverAssignmentService driverAssignmentService;

    public DriverMatchingConsumer(DriverAssignmentService driverAssignmentService) {
        this.driverAssignmentService = driverAssignmentService;
    }

    @KafkaListener(topics = KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC)
    public void handleRideRequested(RideRequestedEvent event) {
        DriverAssignedEvent assignment = driverAssignmentService.assignDriver(event);
        System.out.printf(
                "MATCHING: Driver %s (%s) assigned to ride %s%n",
                assignment.driverName(),
                assignment.driverId(),
                assignment.rideId()
        );
    }
}

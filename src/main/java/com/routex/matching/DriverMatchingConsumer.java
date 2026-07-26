package com.routex.matching;

import com.routex.dispatch.KafkaTopicConfiguration;
import com.routex.dispatch.RideRequestedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DriverMatchingConsumer {
    private final DriverAssignmentService driverAssignmentService;
    private final DriverAssignmentProducer driverAssignmentProducer;


    public DriverMatchingConsumer(DriverAssignmentService driverAssignmentService, DriverAssignmentProducer driverAssignmentProducer) {
        this.driverAssignmentService = driverAssignmentService;
        this.driverAssignmentProducer = driverAssignmentProducer;
    }

    @KafkaListener(topics = KafkaTopicConfiguration.RIDE_REQUESTED_TOPIC, groupId = "driver-matching-group")
    public void handleRideRequested(RideRequestedEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.OFFSET) long offset) {
        System.out.printf(
                "MATCHING | ride=%s | partition=%d | offset=%d%n",
                event.rideId(),
                partition,
                offset
        );
        DriverAssignmentEvent assignment = driverAssignmentService.assignDriver(event);
//        System.out.printf(
//                "MATCHING: Driver %s (%s) assigned to ride %s%n",
//                assignment.driverName(),
//                assignment.driverId(),
//                assignment.rideId()
//        );
        driverAssignmentProducer.publish(assignment);

    }
}

package com.routex.notification;

import com.routex.dispatch.KafkaTopicConfiguration;
import com.routex.matching.DriverAssignmentEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DriverAssignmentNotificationConsumer {
    @KafkaListener(topics = KafkaTopicConfiguration.DRIVER_ASSIGNED_TOPIC, groupId = "notification-group")
    public void handleDriverAssigned(
            DriverAssignmentEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        System.out.printf(
                "NOTIFICATION: Passenger %s - Driver %s (%s) has been assigned to ride %s%n",
                event.passengerId(),
                event.driverName(),
                event.vehicleNumber(),
                event.rideId()
        );
        String thread = Thread.currentThread().getName();
        System.out.printf(
                "NOTIFICATION | ride=%s | partition=%d | offset=%d | thread=%s%n",
                event.rideId(),
                partition,
                offset,
                thread
        );
        acknowledgment.acknowledge();
    }
}

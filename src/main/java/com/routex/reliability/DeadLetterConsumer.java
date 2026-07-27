package com.routex.reliability;

import com.routex.dispatch.KafkaTopicConfiguration;
import com.routex.dispatch.RideRequestedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterConsumer {

    @KafkaListener(
            topics = KafkaTopicConfiguration.RIDE_REQUESTED_DLT,
            groupId = "routex-dlt-monitor"
    )
    public void handleDeadLetter(
            ConsumerRecord<String, RideRequestedEvent> record) {

        RideRequestedEvent event = record.value();

        System.out.printf(
                "DLT | ride=%s | passenger=%s | partition=%d | offset=%d%n",
                event.rideId(),
                event.passengerId(),
                record.partition(),
                record.offset()
        );

        record.headers().forEach(header ->
                System.out.printf(
                        "DLT HEADER | %s = %s%n",
                        header.key(),
                        new String(header.value())
                )
        );
    }
}
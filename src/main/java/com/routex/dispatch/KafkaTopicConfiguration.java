package com.routex.dispatch;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfiguration {

    public static final String RIDE_REQUESTED_TOPIC = "ride-requested";
    public static final String DRIVER_ASSIGNED_TOPIC = "driver-assigned";

    @Bean
    NewTopic rideRequestedTopic() {
        return TopicBuilder.name(RIDE_REQUESTED_TOPIC).build();
    }
    @Bean
    NewTopic driverAssignedTopic() {
        return TopicBuilder.name(DRIVER_ASSIGNED_TOPIC).build();
    }
}


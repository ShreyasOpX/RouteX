package com.routex.matching;
import com.routex.dispatch.KafkaTopicConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DriverAssignmentProducer {
    private final KafkaTemplate<String , Object> kafkaTemplate;
    public DriverAssignmentProducer(KafkaTemplate<String , Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void publish(DriverAssignmentEvent event){
        kafkaTemplate.send(KafkaTopicConfiguration.DRIVER_ASSIGNED_TOPIC, event.rideId(), event);
    }
}

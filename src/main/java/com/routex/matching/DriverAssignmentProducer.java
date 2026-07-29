package com.routex.matching;
import com.routex.dispatch.KafkaTopicConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;

@Component
public class DriverAssignmentProducer {
    private final KafkaTemplate<String , Object> kafkaTemplate;
    public DriverAssignmentProducer(KafkaTemplate<String , Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void publish(DriverAssignmentEvent event){
        //kafkaTemplate.send(KafkaTopicConfiguration.DRIVER_ASSIGNED_TOPIC, event.rideId(), event);
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(
                        KafkaTopicConfiguration.DRIVER_ASSIGNED_TOPIC,
                        event.rideId(),
                        event
                );

        future.whenComplete((result, ex) -> {

            if (ex != null) {
                System.err.printf(
                        "PRODUCER ERROR | topic=%s | key=%s | error=%s%n",
                        KafkaTopicConfiguration.DRIVER_ASSIGNED_TOPIC,
                        event.rideId(),
                        ex.getMessage()
                );
                return;
            }

            var metadata = result.getRecordMetadata();

            System.out.printf(
                    "PRODUCER ACK | topic=%s | partition=%d | offset=%d | key=%s%n",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    event.rideId()
            );
        });
    }
}

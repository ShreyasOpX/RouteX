package com.routex.reliability;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class KafkaRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf(
                "REBALANCE | REVOKED | thread=%s | partitions=%s%n",
                Thread.currentThread().getName(),
                partitions
        );
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                System.out.printf(
                        "REBALANCE | ASSIGNED | thread=%s | partitions=%s%n",
                        Thread.currentThread().getName(),
                        partitions
                );
    }
}

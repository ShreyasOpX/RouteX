package com.routex.monitoring;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class ConsumerLagMonitor {
    private static final String GROUP_ID = "driver-matching-group";
    private final AdminClient adminClient;

    public ConsumerLagMonitor(AdminClient adminClient) {
        this.adminClient = adminClient;
    }
    @Scheduled(fixedDelay = 10000)
    public  void monitorLag() {
        try{
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(GROUP_ID);
            Map<TopicPartition, OffsetAndMetadata> commitedOffsets = offsetsResult.partitionsToOffsetAndMetadata().get();
            if(commitedOffsets.isEmpty()){
                return;
            }
            Map<TopicPartition, OffsetSpec> latestOffsetsRequest = commitedOffsets.keySet()
                    .stream()
                    .collect(Collectors.toMap(
                            tp -> tp,
                            tp -> OffsetSpec.latest()
                    ));
            Map<TopicPartition,
                    ListOffsetsResult.ListOffsetsResultInfo>
                    latestOffsets =
                    adminClient.listOffsets(latestOffsetsRequest)
                            .all()
                            .get();
            System.out.println();
            System.out.println("========== CONSUMER LAG ==========");

            commitedOffsets.forEach((tp, committed) -> {

                long endOffset = latestOffsets.get(tp).offset();
                long lag = endOffset - committed.offset();

                System.out.printf(
                        """
                        Group      : %s
                        Topic      : %s
                        Partition  : %d
                        Committed  : %d
                        End Offset : %d
                        Lag        : %d
                        
                        """,
                        GROUP_ID,
                        tp.topic(),
                        tp.partition(),
                        committed.offset(),
                        endOffset,
                        lag
                );
            });

            System.out.println("==================================");
            System.out.println();
        }catch (InterruptedException | ExecutionException e) {
            System.err.println("Unable to fetch consumer lag.");
            e.printStackTrace();
        }
    }
}

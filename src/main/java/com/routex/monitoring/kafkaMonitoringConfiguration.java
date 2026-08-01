package com.routex.monitoring;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;


@Configuration
public class kafkaMonitoringConfiguration {
    @Bean
    public AdminClient adminClient(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        return AdminClient.create(
                Map.of(
                        "bootstrap.servers", bootstrapServers
                )
        );
    }
}

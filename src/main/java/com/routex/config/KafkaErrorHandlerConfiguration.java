package com.routex.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfiguration {
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3L);
        return new DefaultErrorHandler(fixedBackOff);
    }


}

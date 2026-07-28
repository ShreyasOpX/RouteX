package com.routex.config;


import com.routex.reliability.KafkaRebalanceListener;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

@Configuration
public class KafkaConsumerConfiguration {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?>
    driverMatchingKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            org.springframework.kafka.core.ConsumerFactory<Object, Object> consumerFactory,
            KafkaRebalanceListener rebalanceListener) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        configurer.configure(factory, consumerFactory);

        factory.getContainerProperties()
                .setConsumerRebalanceListener(rebalanceListener);

        return factory;
    }
}

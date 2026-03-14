package com.learnkafka.config;

import com.learnkafka.dto.LibraryEventDto;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>> kafkaListenerContainerFactory(
            ConsumerFactory<Integer, LibraryEventDto> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
        factory.setConsumerFactory(consumerFactory);

        // Default: AckMode.BATCH — offsets committed after all records from poll() are processed
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        // Manual: offsets committed only when Acknowledgment.acknowledge() is called
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }
}


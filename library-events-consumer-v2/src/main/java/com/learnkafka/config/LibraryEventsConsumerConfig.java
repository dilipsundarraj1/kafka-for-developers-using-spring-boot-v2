package com.learnkafka.config;

import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.service.FailureRecordService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    private final FailureRecordService failureRecordService;

    public LibraryEventsConsumerConfig(FailureRecordService failureRecordService) {
        this.failureRecordService = failureRecordService;
    }

    // ── Error Handler ────────────────────────────────────────────────────────
    // Retries up to 3 times with 1-second fixed backoff.
    // On exhaustion, persists the failed record to the failure_record table (OPEN).
    // A scheduler later picks up OPEN records and retries them.

    @Bean
    public DefaultErrorHandler errorHandler() {

        // Retry 3 times, wait 1 second between attempts
        var fixedBackOff = new FixedBackOff(1_000L, 3L);

        // Recovery: persist to DB failure table instead of (or in addition to) DLT
        var errorHandler = new DefaultErrorHandler(
                (record, exception) -> {
                    log.error("All retries exhausted. Persisting failed record to failure_record table. "
                                    + "Topic={}, Partition={}, Offset={}, Exception={}",
                            record.topic(), record.partition(), record.offset(), exception.getMessage());

                    //noinspection unchecked
                    failureRecordService.saveFailureRecord(
                            (org.apache.kafka.clients.consumer.ConsumerRecord<Integer, LibraryEventDto>) record,
                            exception
                    );
                },
                fixedBackOff
        );

        // These exceptions skip retries and go straight to the recoverer
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,          // bad payload — will never succeed
                DataIntegrityViolationException.class    // duplicate key — always fails
        );

        // Log each retry attempt
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for record. Topic={}, Partition={}, Offset={}, Error={}",
                        deliveryAttempt,
                        record.topic(), record.partition(), record.offset(),
                        ex.getMessage())
        );

        return errorHandler;
    }

    // ── Container Factory ────────────────────────────────────────────────────

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>> kafkaListenerContainerFactory(
            ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
            DefaultErrorHandler errorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
        factory.setConsumerFactory(consumerFactory);

        // Default: AckMode.BATCH — offsets committed after all records from poll() are processed
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        // Manual: offsets committed only when Acknowledgment.acknowledge() is called
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}

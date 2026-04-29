package com.learnkafka.config;

import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.service.FailureRecordService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    private final FailureRecordService failureRecordService;
    private final String recoveryMode;
    private final String bootstrapServers;

    public LibraryEventsConsumerConfig(
            FailureRecordService failureRecordService,
            @Value("${app.kafka.recovery.mode:failure-table}") String recoveryMode,
            @Value("${spring.kafka.consumer.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        this.failureRecordService = failureRecordService;
        this.recoveryMode = recoveryMode;
        this.bootstrapServers = bootstrapServers;
    }

    @Bean
    public ProducerFactory<Integer, Object> dltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<Integer, Object> dltKafkaTemplate(ProducerFactory<Integer, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<Integer, Object> dltKafkaTemplate) {
        return new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );
    }

    @Bean
    public ConsumerRecordRecoverer consumerRecordRecoverer(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        RecoveryMode mode = RecoveryMode.from(recoveryMode);
        log.info("Kafka recovery mode: {}", mode);

        return (record, exception) -> {
            switch (mode) {
                case DLT -> publishToDlt(record, exception, deadLetterPublishingRecoverer);
                case BOTH -> {
                    persistFailureRecord(record, exception);
                    publishToDlt(record, exception, deadLetterPublishingRecoverer);
                }
                case FAILURE_TABLE -> persistFailureRecord(record, exception);
            }
        };
    }

    // ── Error Handler ────────────────────────────────────────────────────────
    // Retries up to 3 times with 1-second fixed backoff.
    // On exhaustion, recovery strategy is selected by app.kafka.recovery.mode
    // (failure-table | dlt | both).

    @Bean
    public DefaultErrorHandler errorHandler(ConsumerRecordRecoverer consumerRecordRecoverer) {

        // Retry 3 times, wait 1 second between attempts
        var fixedBackOff = new FixedBackOff(1_000L, 3L);

        var errorHandler = new DefaultErrorHandler(consumerRecordRecoverer, fixedBackOff);

        // These exceptions skip retries and go straight to the recoverer
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,         // malformed JSON / type mismatch
                IllegalArgumentException.class,          // bad payload — will never succeed
                DataIntegrityViolationException.class    // duplicate key — always fails
        );

        // Consumer error listener — covers the full retry lifecycle
        errorHandler.setRetryListeners(new RetryListener() {

            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Delivery attempt {} failed. Topic={}, Partition={}, Offset={}, Error={}",
                        deliveryAttempt,
                        record.topic(), record.partition(), record.offset(),
                        ex.getMessage(), ex);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.info("Record recovered after retries. Topic={}, Partition={}, Offset={}",
                        record.topic(), record.partition(), record.offset());
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("Record recovery failed. Topic={}, Partition={}, Offset={}, OriginalError={}, RecoveryError={}",
                        record.topic(), record.partition(), record.offset(),
                        original.getMessage(), failure.getMessage(), original);
            }
        });

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

    private void persistFailureRecord(ConsumerRecord<?, ?> record, Exception exception) {
        log.error("All retries exhausted. Persisting failed record to failure_record table. "
                        + "Topic={}, Partition={}, Offset={}, Exception={}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());

        //noinspection unchecked
        failureRecordService.saveFailureRecord(
                (ConsumerRecord<Integer, LibraryEventDto>) record,
                exception
        );
    }

    private void publishToDlt(
            ConsumerRecord<?, ?> record,
            Exception exception,
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        log.error("All retries exhausted. Publishing failed record to DLT. Topic={}, Partition={}, Offset={}, Exception={}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());
        deadLetterPublishingRecoverer.accept(record, exception);
    }

    private enum RecoveryMode {
        FAILURE_TABLE,
        DLT,
        BOTH;

        private static RecoveryMode from(String value) {
            String normalized = value == null ? "" : value.trim().toUpperCase().replace('-', '_');
            return switch (normalized) {
                case "FAILURE_TABLE" -> FAILURE_TABLE;
                case "DLT" -> DLT;
                case "BOTH" -> BOTH;
                default -> throw new IllegalArgumentException(
                        "Invalid app.kafka.recovery.mode: " + value + " (expected: failure-table, dlt, both)");
            };
        }
    }
}

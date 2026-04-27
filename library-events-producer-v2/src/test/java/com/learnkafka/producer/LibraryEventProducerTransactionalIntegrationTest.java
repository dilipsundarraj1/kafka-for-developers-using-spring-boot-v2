package com.learnkafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.config.AppConstants;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.exception.LibraryEventPublishException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@SpringBootTest(properties = {
        "spring.profiles.active=test",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.topic=" + AppConstants.DEFAULT_LIBRARY_EVENTS_TOPIC,
        "spring.kafka.producer.transaction-id-prefix=library-events-tx-test-"
})
@EmbeddedKafka(
        partitions = 1,
        topics = AppConstants.DEFAULT_LIBRARY_EVENTS_TOPIC,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "min.insync.replicas=1"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LibraryEventProducerTransactionalIntegrationTest {

    private static final String TOPIC = AppConstants.DEFAULT_LIBRARY_EVENTS_TOPIC;

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    private Consumer<Long, String> readCommittedConsumer;

    @BeforeEach
    void setUp() {
        var consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "library-events-producer-tx-int-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        readCommittedConsumer = new DefaultKafkaConsumerFactory<Long, String>(consumerProps)
                .createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(readCommittedConsumer, TOPIC);
    }

    @AfterEach
    void tearDown() {
        if (readCommittedConsumer != null) {
            readCommittedConsumer.close();
        }
    }

    @Test
    void sendLibraryEventWithTransactionalAnnotation_whenAbortScenarioTriggered_doesNotPublishCommittedRecord() {
        LibraryEvent abortEvent = buildAbortEvent(1001L, 501L, "Author One");

        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventWithTransactionalAnnotation(abortEvent))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessageContaining("Simulated transaction abort");

        assertThat(findCommittedRecord(abortEvent)).isNull();
    }

    @Test
    void sendLibraryEventSynchronousWithTransactionalAnnotation_whenAbortScenarioTriggered_doesNotPublishCommittedRecord() {
        LibraryEvent abortEvent = buildAbortEvent(1002L, 502L, "Author Two");

        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventSynchronousWithTransactionalAnnotation(abortEvent))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessageContaining("Simulated transaction abort");

        assertThat(findCommittedRecord(abortEvent)).isNull();
    }

    @Test
    void sendLibraryEventTransactionalAsync_whenAbortScenarioTriggered_doesNotPublishCommittedRecord() {
        LibraryEvent abortEvent = buildAbortEvent(1003L, 503L, "Author Three");

        Throwable thrown = catchThrowable(() -> libraryEventProducer.sendLibraryEventTransactionalAsync(abortEvent).join());

        assertThat(thrown).isNotNull();
        assertThat(thrown.getCause())
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessageContaining("Failed to publish LibraryEvent transactionally (async)");

        assertThat(findCommittedRecord(abortEvent)).isNull();
    }

    @Test
    void sendLibraryEventTransactional_whenAbortScenarioTriggered_doesNotPublishCommittedRecord() {
        LibraryEvent abortEvent = buildAbortEvent(1004L, 504L, "Author Four");

        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventTransactional(abortEvent))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessageContaining("Simulated transaction abort");

        assertThat(findCommittedRecord(abortEvent)).isNull();
    }

    private LibraryEvent buildAbortEvent(Long libraryEventId, Long bookId, String author) {
        return new LibraryEvent(
                libraryEventId,
                LibraryEventType.UPDATE,
                new Book(bookId, "transaction", author)
        );
    }

    private ConsumerRecord<Long, String> findCommittedRecord(LibraryEvent expectedEvent) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();

        while (System.nanoTime() < deadline) {
            var records = readCommittedConsumer.poll(Duration.ofMillis(250));

            for (ConsumerRecord<Long, String> record : records.records(TOPIC)) {
                try {
                    LibraryEvent publishedEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
                    if (publishedEvent.equals(expectedEvent)) {
                        return record;
                    }
                } catch (Exception ex) {
                    throw new AssertionError("Failed to deserialize consumed Kafka record", ex);
                }
            }
        }

        return null;
    }

    @TestConfiguration
    static class TestKafkaTemplateConfig {
        @Bean
        @Primary
        KafkaTemplate<Long, LibraryEvent> kafkaTemplate(ProducerFactory<Long, LibraryEvent> producerFactory) {
            KafkaTemplate<Long, LibraryEvent> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            kafkaTemplate.setAllowNonTransactional(true);
            return kafkaTemplate;
        }
    }
}


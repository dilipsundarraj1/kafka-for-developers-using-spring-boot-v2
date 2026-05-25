package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.config.AppConstants;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
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
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.profiles.active=test",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.topic=" + AppConstants.DEFAULT_LIBRARY_EVENTS_TOPIC
})
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = AppConstants.DEFAULT_LIBRARY_EVENTS_TOPIC)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LibraryEventsControllerIntegrationTest {

    private static final String TOPIC = AppConstants.DEFAULT_LIBRARY_EVENTS_TOPIC;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Long, String> consumer;

    @BeforeEach
    void setUp() {
        var consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "library-events-controller-int-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new DefaultKafkaConsumerFactory<Long, String>(consumerProps)
                .createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, TOPIC);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void postLibraryEvent_returns201_andPublishesEventToKafka() throws Exception {
        LibraryEvent requestEvent = new LibraryEvent(
                null,
                LibraryEventType.ADD,
                new Book(123L, "Kafka Using Spring Boot", "Dilip")
        );

        MvcResult mvcResult = mockMvc.perform(post(AppConstants.API_BASE_PATH)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestEvent)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(123))
                .andExpect(jsonPath("$.book.bookName").value("Kafka Using Spring Boot"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Dilip"));

        ConsumerRecord<Long, String> consumerRecord = pollForExpectedRecord(requestEvent);
        LibraryEvent publishedEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        assertThat(consumerRecord.key()).isNull();
        assertThat(publishedEvent).isEqualTo(requestEvent);
    }

    @Test
    void postLibraryEvent_returns201_andPublishesDifferentEventToKafka() throws Exception {
        LibraryEvent requestEvent = new LibraryEvent(
                null,
                LibraryEventType.ADD,
                new Book(456L, "Spring Boot with Kafka Streams", "Jane Doe")
        );

        MvcResult mvcResult = mockMvc.perform(post(AppConstants.API_BASE_PATH)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestEvent)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(456))
                .andExpect(jsonPath("$.book.bookName").value("Spring Boot with Kafka Streams"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Jane Doe"));

        ConsumerRecord<Long, String> consumerRecord = pollForExpectedRecord(requestEvent);
        LibraryEvent publishedEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        assertThat(consumerRecord.key()).isNull();
        assertThat(publishedEvent).isEqualTo(requestEvent);
    }

    @Test
    void putLibraryEvent_returns200_andPublishesUpdateEventToKafka() throws Exception {
        LibraryEvent requestEvent = new LibraryEvent(
                999L,
                LibraryEventType.UPDATE,
                new Book(777L, "Distributed Systems Patterns", "Alex Smith")
        );

        MvcResult mvcResult = mockMvc.perform(put(AppConstants.API_BASE_PATH)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestEvent)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(999))
                .andExpect(jsonPath("$.eventType").value("UPDATE"))
                .andExpect(jsonPath("$.book.bookId").value(777))
                .andExpect(jsonPath("$.book.bookName").value("Distributed Systems Patterns"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Alex Smith"));

        ConsumerRecord<Long, String> consumerRecord = pollForExpectedRecord(requestEvent);
        LibraryEvent publishedEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        assertThat(consumerRecord.key()).isEqualTo(requestEvent.libraryEventId());
        assertThat(publishedEvent).isEqualTo(requestEvent);
    }

    @Test
    void putLibraryEvent_returns200_andPublishesDifferentUpdateEventToKafka() throws Exception {
        LibraryEvent requestEvent = new LibraryEvent(
                1001L,
                LibraryEventType.UPDATE,
                new Book(888L, "Kafka Internals", "Maria Garcia")
        );

        MvcResult mvcResult = mockMvc.perform(put(AppConstants.API_BASE_PATH)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestEvent)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(1001))
                .andExpect(jsonPath("$.eventType").value("UPDATE"))
                .andExpect(jsonPath("$.book.bookId").value(888))
                .andExpect(jsonPath("$.book.bookName").value("Kafka Internals"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Maria Garcia"));

        ConsumerRecord<Long, String> consumerRecord = pollForExpectedRecord(requestEvent);
        LibraryEvent publishedEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        assertThat(consumerRecord.key()).isEqualTo(requestEvent.libraryEventId());
        assertThat(publishedEvent).isEqualTo(requestEvent);
    }

    private ConsumerRecord<Long, String> pollForExpectedRecord(LibraryEvent expectedEvent) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();

        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<Long, String> record : records.records(TOPIC)) {
                LibraryEvent publishedEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
                if (publishedEvent.equals(expectedEvent)) {
                    return record;
                }
            }
        }

        throw new AssertionError("Expected LibraryEvent was not published to Kafka within the timeout");
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


package com.learnkafka.controller;

import tools.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.util.TestUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test using Spring Boot Docker Compose support.
 * Kafka is started automatically from compose.yaml when tests run.
 * Uses 'test' profile to load application-test.yml configuration.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("test")
public class LibraryEventsControllerIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "library-events";

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        // Create consumer pointing to Docker Compose Kafka with unique group ID
        Map<String, Object> configs = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
                ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs).createConsumer();
        consumer.subscribe(List.of(TOPIC));
        // Poll once to assign partitions and set position to end
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void postLibraryEvent() {
        //given
        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();
        System.out.println("libraryEvent : " + objectMapper.writeValueAsString(libraryEvent));
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        //when
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);

        //then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        // Read the record from Kafka and verify
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        assertTrue(consumerRecords.count() >= 1, "Expected at least 1 record");

        AtomicBoolean found = new AtomicBoolean(false);
        consumerRecords.forEach(record -> {
            var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            if (libraryEventActual.libraryEventType() == LibraryEventType.NEW) {
                assertEquals(libraryEvent, libraryEventActual);
                found.set(true);
            }
        });
        assertTrue(found.get(), "Expected to find a NEW library event");
    }

    @Test
    void putLibraryEvent() {
        //given
        var libraryEventUpdate = TestUtil.libraryEventRecordUpdate();
        System.out.println("libraryEvent : " + objectMapper.writeValueAsString(libraryEventUpdate));
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEventUpdate, headers);

        //when
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, request, LibraryEvent.class);

        //then
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        // Read the record from Kafka and verify
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        assertTrue(consumerRecords.count() >= 1, "Expected at least 1 record");

        AtomicBoolean found = new AtomicBoolean(false);
        consumerRecords.forEach(record -> {
            var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            if (libraryEventActual.libraryEventType() == LibraryEventType.UPDATE && record.key() != null) {
                assertEquals(libraryEventUpdate, libraryEventActual);
                found.set(true);
            }
        });
        assertTrue(found.get(), "Expected to find an UPDATE library event");
    }
}

package com.learnkafka.consumer;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEventDTO;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventFailureRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"library-events", "library-events.DLT"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers")
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.producer.bootstrap-servers=${spring.kafka.consumer.bootstrap-servers}",
        "app.kafka.recovery.mode=both"
})
@ImportTestcontainers
class LibraryEventsConsumerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private LibraryEventFailureRepository libraryEventFailureRepository;

    @Autowired
    private Environment environment;

    private KafkaTemplate<Integer, LibraryEventDTO> kafkaTemplate;

    @BeforeEach
    void setUp() {
        libraryEventFailureRepository.deleteAll();
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        var producerFactory = new DefaultKafkaProducerFactory<Integer, LibraryEventDTO>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
        Book book = new Book(100L, "Clean Code", "Robert C. Martin");
        LibraryEventDTO dto = new LibraryEventDTO(null, LibraryEventType.ADD, book);

        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);
        waitForRecordCount(1, 10);

        var events = libraryEventRepository.findAll();
        var books = bookRepository.findAll();

        assertEquals(1, events.size());
        assertEquals(1, books.size());

        var savedEvent = events.getFirst();
        var savedBook = books.getFirst();

        assertNotNull(savedEvent.getLibraryEventId());
        assertEquals(LibraryEventType.ADD, savedEvent.getEventType());
        assertNotNull(savedEvent.getCreatedAt());
        assertNotNull(savedEvent.getUpdatedAt());

        assertEquals(100, savedBook.getBookId());
        assertEquals("Clean Code", savedBook.getBookName());
        assertEquals("Robert C. Martin", savedBook.getBookAuthor());
        assertNotNull(savedBook.getLibraryEvent());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
        assertNotNull(savedBook.getCreatedAt());
        assertNotNull(savedBook.getUpdatedAt());
    }

    @Test
    void consumeLibraryEvent_UPDATE_shouldUpdateExistingLibraryEventAndBook() throws Exception {
        Book addBook = new Book(101L, "Kafka Basics", "Alice");
        LibraryEventDTO addDto = new LibraryEventDTO(null, LibraryEventType.ADD, addBook);
        kafkaTemplate.send("library-events", addDto).get(10, TimeUnit.SECONDS);

        waitForRecordCount(1, 10);
        Integer existingId = libraryEventRepository.findAll().getFirst().getLibraryEventId();

        Book updateBook = new Book(101L, "Kafka in Action", "Bob");
        LibraryEventDTO updateDto = new LibraryEventDTO(existingId.longValue(), LibraryEventType.UPDATE, updateBook);
        kafkaTemplate.send("library-events", updateDto).get(10, TimeUnit.SECONDS);

        waitForCondition(() -> libraryEventRepository.findById(existingId)
                        .map(event -> event.getEventType() == LibraryEventType.UPDATE
                                && event.getBook() != null
                                && "Kafka in Action".equals(event.getBook().getBookName())
                                && "Bob".equals(event.getBook().getBookAuthor()))
                        .orElse(false),
                10,
                "Timed out waiting for UPDATE event to be persisted");

        assertEquals(1, libraryEventRepository.count());
        assertEquals(1, bookRepository.count());
        assertEquals("Kafka in Action", bookRepository.findById(101).orElseThrow().getBookName());
    }

    @Test
    void consumeLibraryEvent_invalid_withNullBook_shouldNotPersistAnyRecord() throws Exception {
        LibraryEventDTO invalidDto = new LibraryEventDTO(null, LibraryEventType.ADD, null);

        kafkaTemplate.send("library-events", invalidDto).get(10, TimeUnit.SECONDS);

        assertConditionRemainsTrue(
                () -> libraryEventRepository.count() == 0 && bookRepository.count() == 0,
                3,
                "Unexpected persistence happened for invalid ADD event with null book");

        waitForCondition(
                () -> libraryEventFailureRepository.count() == 1,
                10,
                "Timed out waiting for failure-table record for invalid ADD event");

        var failure = libraryEventFailureRepository.findAll().getFirst();
        assertEquals("library-events", failure.getTopic());
        assertNotNull(failure.getStackTrace());
        assertTrue(failure.getExceptionClass().contains("ListenerExecutionFailedException"));
        assertTrue(failure.getStackTrace().contains("Validation failed:"));

    }

    @Test
    void consumeLibraryEvent_UPDATE_withNullLibraryEventId_shouldNotUpdateExistingRecord() throws Exception {
        Book addBook = new Book(102L, "Refactoring", "Martin Fowler");
        LibraryEventDTO addDto = new LibraryEventDTO(null, LibraryEventType.ADD, addBook);
        kafkaTemplate.send("library-events", addDto).get(10, TimeUnit.SECONDS);

        waitForRecordCount(1, 10);

        Integer existingId = libraryEventRepository.findAll().getFirst().getLibraryEventId();
        String originalBookName = bookRepository.findById(102).orElseThrow().getBookName();

        Book invalidUpdateBook = new Book(102L, "Refactoring 2nd Edition", "Martin Fowler");
        LibraryEventDTO invalidUpdateDto = new LibraryEventDTO(null, LibraryEventType.UPDATE, invalidUpdateBook);
        kafkaTemplate.send("library-events", invalidUpdateDto).get(10, TimeUnit.SECONDS);

        assertConditionRemainsTrue(
                () -> libraryEventRepository.count() == 1
                        && bookRepository.count() == 1
                        && libraryEventRepository.findById(existingId)
                        .map(event -> event.getEventType() == LibraryEventType.ADD)
                        .orElse(false)
                        && bookRepository.findById(102)
                        .map(book -> originalBookName.equals(book.getBookName()))
                        .orElse(false),
                3,
                "Invalid UPDATE with null libraryEventId unexpectedly changed persisted data");
    }

    @Test
    void consumeLibraryEvent_UPDATE_whenLibraryEventDoesNotExist_shouldRecoverToDltAndFailureTable() throws Exception {
        assertEquals("both", environment.getProperty("app.kafka.recovery.mode"));

        Book updateBook = new Book(777L, "Missing Event", "No Author");
        LibraryEventDTO updateDto = new LibraryEventDTO(9999L, LibraryEventType.UPDATE, updateBook);

        kafkaTemplate.send("library-events", updateDto).get(10, TimeUnit.SECONDS);

        waitForCondition(
                () -> libraryEventFailureRepository.count() == 1,
                10,
                "Timed out waiting for failure-table record for missing UPDATE event");

        assertEquals(0, libraryEventRepository.count());
        assertEquals(0, bookRepository.count());

        var failure = libraryEventFailureRepository.findAll().getFirst();
        assertEquals("library-events", failure.getTopic());
        assertTrue(failure.getExceptionClass().contains("ListenerExecutionFailedException"));
        assertTrue(failure.getStackTrace().contains("LibraryEvent not found for update"));

        try (KafkaConsumer<Integer, String> dltConsumer = createDltConsumer()) {
            dltConsumer.subscribe(List.of("library-events.DLT"));
            ConsumerRecord<Integer, String> dltRecord = waitForDltRecord(
                    dltConsumer,
                    value -> value.contains("\"libraryEventId\":9999"),
                    10);

            assertNotNull(dltRecord);
            assertEquals("library-events.DLT", dltRecord.topic());
            assertTrue(dltRecord.value().contains("\"libraryEventId\":9999"));
            assertTrue(dltRecord.value().contains("\"eventType\":\"UPDATE\""));
        }
    }

    private KafkaConsumer<Integer, String> createDltConsumer() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }

    private ConsumerRecord<Integer, String> waitForDltRecord(KafkaConsumer<Integer, String> consumer,
                                                             java.util.function.Predicate<String> valueMatcher,
                                                             int timeoutSeconds) {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<Integer, String> record : records) {
                if (valueMatcher.test(record.value())) {
                    return record;
                }
            }
        }
        fail("Timed out waiting for matching message in library-events.DLT");
        return null;
    }

    private void waitForRecordCount(long expectedCount, int timeoutSeconds) throws InterruptedException {
        waitForCondition(() -> libraryEventRepository.count() >= expectedCount,
                timeoutSeconds,
                "Timed out waiting for " + expectedCount + " record(s), found " + libraryEventRepository.count());
        Thread.sleep(200);
    }

    private void waitForCondition(BooleanSupplier condition,
                                  int timeoutSeconds,
                                  String failureMessage) throws InterruptedException {
        for (int i = 0; i < timeoutSeconds * 10; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        fail(failureMessage);
    }

    private void assertConditionRemainsTrue(BooleanSupplier condition,
                                            int durationSeconds,
                                            String failureMessage) throws InterruptedException {
        for (int i = 0; i < durationSeconds * 10; i++) {
            assertTrue(condition.getAsBoolean(), failureMessage);
            Thread.sleep(100);
        }
    }
}


package com.learnkafka.consumer;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers")
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest"
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

    private KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        var producerFactory = new DefaultKafkaProducerFactory<Integer, LibraryEventDto>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
        // given
        BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
        LibraryEventDto libraryEventDto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

        // when — produce to embedded Kafka
        kafkaTemplate.send("library-events", libraryEventDto).get(10, TimeUnit.SECONDS);

        // then — wait for consumer to process and persist
        waitForRecordCount(1, 10);

        List<LibraryEvent> libraryEvents = libraryEventRepository.findAll();
        assertEquals(1, libraryEvents.size());

        LibraryEvent savedEvent = libraryEvents.getFirst();
        assertNotNull(savedEvent.getLibraryEventId());
        assertEquals(LibraryEventType.ADD, savedEvent.getEventType());
        assertNotNull(savedEvent.getCreatedAt());
        assertNotNull(savedEvent.getUpdatedAt());

        List<Book> books = bookRepository.findAll();
        assertEquals(1, books.size());

        Book savedBook = books.getFirst();
        assertEquals(1, savedBook.getBookId());
        assertEquals("Clean Code", savedBook.getBookName());
        assertEquals("Robert C. Martin", savedBook.getBookAuthor());
        assertNotNull(savedBook.getCreatedAt());
        assertNotNull(savedBook.getUpdatedAt());

        // Verify FK relationship — Book references LibraryEvent
        assertNotNull(savedBook.getLibraryEvent());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
    }

    @Test
    void consumeLibraryEvent_ADD_multipleMessages_shouldPersistAll() throws Exception {
        // given
        BookDto bookDto1 = new BookDto(10, "Clean Code", "Robert C. Martin");
        LibraryEventDto dto1 = new LibraryEventDto(null, LibraryEventType.ADD, bookDto1);

        BookDto bookDto2 = new BookDto(20, "Effective Java", "Joshua Bloch");
        LibraryEventDto dto2 = new LibraryEventDto(null, LibraryEventType.ADD, bookDto2);

        // when — produce two messages
        kafkaTemplate.send("library-events", dto1).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send("library-events", dto2).get(10, TimeUnit.SECONDS);

        // then — both should be consumed and persisted
        waitForRecordCount(2, 10);

        assertEquals(2, libraryEventRepository.count());
        assertEquals(2, bookRepository.count());

        assertTrue(bookRepository.findById(10).isPresent());
        assertEquals("Clean Code", bookRepository.findById(10).get().getBookName());

        assertTrue(bookRepository.findById(20).isPresent());
        assertEquals("Effective Java", bookRepository.findById(20).get().getBookName());
    }

    @Test
    void consumeLibraryEvent_UPDATE_shouldPersistLibraryEvent() throws Exception {
        // given
        BookDto bookDto = new BookDto(99, "Design Patterns", "Gang of Four");
        LibraryEventDto libraryEventDto = new LibraryEventDto(null, LibraryEventType.UPDATE, bookDto);

        // when
        kafkaTemplate.send("library-events", libraryEventDto).get(10, TimeUnit.SECONDS);

        // then
        waitForRecordCount(1, 10);

        List<LibraryEvent> libraryEvents = libraryEventRepository.findAll();
        assertEquals(1, libraryEvents.size());
        assertEquals(LibraryEventType.UPDATE, libraryEvents.getFirst().getEventType());

        List<Book> books = bookRepository.findAll();
        assertEquals(1, books.size());
        assertEquals(99, books.getFirst().getBookId());
        assertEquals("Design Patterns", books.getFirst().getBookName());
        assertEquals("Gang of Four", books.getFirst().getBookAuthor());
    }

    @Test
    void consumeLibraryEvent_withKey_shouldPersistSuccessfully() throws Exception {
        // given — producer sends with a Kafka message key
        BookDto bookDto = new BookDto(42, "Refactoring", "Martin Fowler");
        LibraryEventDto libraryEventDto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

        // when — send with an explicit key
        kafkaTemplate.send("library-events", 42, libraryEventDto).get(10, TimeUnit.SECONDS);

        // then
        waitForRecordCount(1, 10);

        List<LibraryEvent> libraryEvents = libraryEventRepository.findAll();
        assertEquals(1, libraryEvents.size());

        LibraryEvent savedEvent = libraryEvents.getFirst();
        assertNotNull(savedEvent.getLibraryEventId());
        assertEquals(LibraryEventType.ADD, savedEvent.getEventType());

        Book savedBook = bookRepository.findById(42).orElse(null);
        assertNotNull(savedBook);
        assertEquals("Refactoring", savedBook.getBookName());
        assertEquals("Martin Fowler", savedBook.getBookAuthor());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
    }

    /**
     * Polls the database until the expected number of LibraryEvent records appear,
     * or fails after the given timeout.
     */
    private void waitForRecordCount(long expectedCount, int timeoutSeconds) throws InterruptedException {
        for (int i = 0; i < timeoutSeconds * 10; i++) {
            if (libraryEventRepository.count() >= expectedCount) {
                // Small buffer for remaining DB operations (Book save after LibraryEvent)
                Thread.sleep(200);
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + expectedCount + " library event(s), found " + libraryEventRepository.count());
    }
}


package com.learnkafka.service;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.dto.LibraryEventMapper;
import com.learnkafka.dto.LibraryEventResponseDto;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventRepository libraryEventRepository;
    private final BookRepository bookRepository;

    public LibraryEventService(LibraryEventRepository libraryEventRepository,
                               BookRepository bookRepository) {
        this.libraryEventRepository = libraryEventRepository;
        this.bookRepository = bookRepository;
    }

    @Transactional
    public void processEvent(ConsumerRecord<Integer, LibraryEventDto> consumerRecord) {
        LibraryEventDto libraryEventDto = consumerRecord.value();
        log.info("LibraryEventDto : {}", libraryEventDto);

        // Validate event type requirements
        if (libraryEventDto.eventType() == LibraryEventType.UPDATE) {
            if (libraryEventDto.libraryEventId() == null || libraryEventDto.libraryEventId() <= 0) {
                throw new IllegalArgumentException("libraryEventId must be > 0 for UPDATE events");
            }
            handleUpdate(libraryEventDto);
        } else if (libraryEventDto.eventType() == LibraryEventType.ADD) {
            handleAdd(libraryEventDto);
        } else {
            throw new IllegalArgumentException("Unknown event type: " + libraryEventDto.eventType());
        }
    }

    private void handleAdd(LibraryEventDto libraryEventDto) {
        log.info("Processing ADD event");
        LibraryEvent libraryEvent = LibraryEventMapper.toEntity(libraryEventDto);
        // For @GeneratedValue IDs, keep ID null so Hibernate uses INSERT, not UPDATE.
        libraryEvent.setLibraryEventId(null);

        // Save LibraryEvent first — it has @GeneratedValue(IDENTITY), DB generates the ID
        libraryEvent.setBook(null); // detach book temporarily to avoid cascade issues on persist
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        // Now save Book with the FK pointing to the persisted LibraryEvent
        Book book = LibraryEventMapper.toBookEntity(libraryEventDto.book());
        book.setLibraryEvent(savedEvent);
        Book savedBook = bookRepository.save(book);

        // Set bidirectional back-reference for in-memory consistency
        savedEvent.setBook(savedBook);

        log.info("Successfully persisted the library event : {}", savedEvent);
    }

    private void handleUpdate(LibraryEventDto libraryEventDto) {
        log.info("Processing UPDATE event for libraryEventId={}", libraryEventDto.libraryEventId());

        // Fetch existing record
        Optional<LibraryEvent> existingEvent = libraryEventRepository.findById(libraryEventDto.libraryEventId());

        if (existingEvent.isEmpty()) {
            throw new IllegalArgumentException(
                    "LibraryEvent not found for update: id=" + libraryEventDto.libraryEventId());
        }

        LibraryEvent libraryEvent = existingEvent.get();

        // Update event type
        libraryEvent.setEventType(libraryEventDto.eventType());

        // Update or create book
        if (libraryEventDto.book() != null) {
            Book existingBook = libraryEvent.getBook();
            if (existingBook != null) {
                // Update existing book
                existingBook.setBookName(libraryEventDto.book().bookName());
                existingBook.setBookAuthor(libraryEventDto.book().bookAuthor());
                bookRepository.save(existingBook);
            } else {
                // Create new book if it didn't exist
                Book newBook = LibraryEventMapper.toBookEntity(libraryEventDto.book());
                newBook.setLibraryEvent(libraryEvent);
                Book savedBook = bookRepository.save(newBook);
                libraryEvent.setBook(savedBook);
            }
        }

        // Save updated event
        LibraryEvent updatedEvent = libraryEventRepository.save(libraryEvent);
        log.info("Successfully updated the library event : {}", updatedEvent);
    }

    public List<LibraryEventResponseDto> findAll() {
        log.info("Fetching all library events");
        return libraryEventRepository.findAll()
                .stream()
                .map(LibraryEventMapper::toLibraryEventResponseDto)
                .toList();
    }

    public Optional<LibraryEventResponseDto> findById(Integer libraryEventId) {
        log.info("Fetching library event with id: {}", libraryEventId);
        return libraryEventRepository.findById(libraryEventId)
                .map(LibraryEventMapper::toLibraryEventResponseDto);
    }
}


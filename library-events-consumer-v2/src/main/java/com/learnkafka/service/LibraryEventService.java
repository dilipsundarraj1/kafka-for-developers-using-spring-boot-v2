package com.learnkafka.service;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.dto.LibraryEventMapper;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

        LibraryEvent libraryEvent = LibraryEventMapper.toEntity(libraryEventDto);

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
}


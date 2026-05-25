package com.learnkafka.service;

import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.BookResponseDto;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.mapper.LibraryEventMapper;
import com.learnkafka.repository.BookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class BookService {

    private static final Logger log = LoggerFactory.getLogger(BookService.class);

    private final BookRepository bookRepository;
    private final LibraryEventMapper libraryEventMapper;

    public BookService(BookRepository bookRepository, LibraryEventMapper libraryEventMapper) {
        this.bookRepository = bookRepository;
        this.libraryEventMapper = libraryEventMapper;
    }

    public List<BookResponseDto> findAll() {
        log.info("Fetching all books");
        return bookRepository.findAll()
                .stream()
                .map(libraryEventMapper::toBookResponseDto)
                .toList();
    }

    public Optional<BookResponseDto> findById(Integer bookId) {
        log.info("Fetching book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(libraryEventMapper::toBookResponseDto);
    }

    @Transactional
    public BookResponseDto create(BookDto bookDto) {
        log.info("Creating book: {}", bookDto);
        Book savedBook = bookRepository.save(libraryEventMapper.toBookEntity(bookDto));
        log.info("Successfully created book with id: {}", savedBook.getBookId());
        return libraryEventMapper.toBookResponseDto(savedBook);
    }

    @Transactional
    public Optional<BookResponseDto> update(Integer bookId, BookDto bookDto) {
        log.info("Updating book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(existingBook -> {
                    existingBook.setBookName(bookDto.bookName());
                    existingBook.setBookAuthor(bookDto.bookAuthor());
                    Book updatedBook = bookRepository.save(existingBook);
                    log.info("Successfully updated book with id: {}", updatedBook.getBookId());
                    return libraryEventMapper.toBookResponseDto(updatedBook);
                });
    }

    @Transactional
    public boolean delete(Integer bookId) {
        log.info("Deleting book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(book -> {
                    LibraryEvent libraryEvent = book.getLibraryEvent();
                    if (libraryEvent != null) {
                        libraryEvent.setBook(null);
                        book.setLibraryEvent(null);
                    }
                    bookRepository.delete(book);
                    log.info("Successfully deleted book with id: {}", bookId);
                    return true;
                })
                .orElse(false);
    }
}


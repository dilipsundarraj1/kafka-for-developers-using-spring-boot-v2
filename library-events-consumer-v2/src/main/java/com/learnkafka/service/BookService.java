package com.learnkafka.service;

import com.learnkafka.domain.Book;
import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.BookResponseDto;
import com.learnkafka.dto.LibraryEventMapper;
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

    public BookService(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    public List<BookResponseDto> findAll() {
        log.info("Fetching all books");
        return bookRepository.findAll()
                .stream()
                .map(LibraryEventMapper::toBookResponseDto)
                .toList();
    }

    public Optional<BookResponseDto> findById(Integer bookId) {
        log.info("Fetching book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(LibraryEventMapper::toBookResponseDto);
    }

    @Transactional
    public BookResponseDto create(BookDto bookDto) {
        log.info("Creating book: {}", bookDto);
        Book book = LibraryEventMapper.toBookEntity(bookDto);
        Book savedBook = bookRepository.save(book);
        log.info("Successfully created book: {}", savedBook);
        return LibraryEventMapper.toBookResponseDto(savedBook);
    }

    @Transactional
    public Optional<BookResponseDto> update(Integer bookId, BookDto bookDto) {
        log.info("Updating book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(existingBook -> {
                    existingBook.setBookName(bookDto.bookName());
                    existingBook.setBookAuthor(bookDto.bookAuthor());
                    Book updatedBook = bookRepository.save(existingBook);
                    log.info("Successfully updated book: {}", updatedBook);
                    return LibraryEventMapper.toBookResponseDto(updatedBook);
                });
    }

    @Transactional
    public boolean delete(Integer bookId) {
        log.info("Deleting book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(book -> {
                    // Break the bidirectional OneToOne reference so that
                    // LibraryEvent's cascade = ALL does not re-persist the book
                    if (book.getLibraryEvent() != null) {
                        book.getLibraryEvent().setBook(null);
                    }
                    bookRepository.delete(book);
                    log.info("Successfully deleted book with id: {}", bookId);
                    return true;
                })
                .orElse(false);
    }
}



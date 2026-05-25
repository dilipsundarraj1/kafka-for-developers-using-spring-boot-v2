package com.learnkafka.controller;

import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.BookResponseDto;
import com.learnkafka.service.BookService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/books")
public class BookController {

    private static final Logger log = LoggerFactory.getLogger(BookController.class);

    private final BookService bookService;

    public BookController(BookService bookService) {
        this.bookService = bookService;
    }

    @GetMapping
    public ResponseEntity<List<BookResponseDto>> getAllBooks() {
        log.info("GET /v1/books");
        return ResponseEntity.ok(bookService.findAll());
    }

    @GetMapping("/{bookId}")
    public ResponseEntity<BookResponseDto> getBookById(@PathVariable Integer bookId) {
        log.info("GET /v1/books/{}", bookId);
        return bookService.findById(bookId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<BookResponseDto> createBook(@RequestBody @Valid BookDto bookDto) {
        log.info("POST /v1/books - {}", bookDto);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(bookService.create(bookDto));
    }

    @PutMapping("/{bookId}")
    public ResponseEntity<BookResponseDto> updateBook(@PathVariable Integer bookId,
                                                      @RequestBody @Valid BookDto bookDto) {
        log.info("PUT /v1/books/{} - {}", bookId, bookDto);
        return bookService.update(bookId, bookDto)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{bookId}")
    public ResponseEntity<Void> deleteBook(@PathVariable Integer bookId) {
        log.info("DELETE /v1/books/{}", bookId);
        if (bookService.delete(bookId)) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.notFound().build();
    }
}


package com.learnkafka.controller;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ImportTestcontainers
class LibraryEventControllerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    // ── GET all ──────────────────────────────────────────────

    @Test
    void getAllLibraryEvents_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAllLibraryEvents_shouldReturnAllLibraryEvents() throws Exception {
        persistLibraryEventWithBook(1L, "Clean Code", "Robert C. Martin");
        persistLibraryEventWithBook(2L, "Effective Java", "Joshua Bloch");

        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void getAllLibraryEvents_shouldIncludeBookDetails() throws Exception {
        persistLibraryEventWithBook(1L, "Clean Code", "Robert C. Martin");

        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$[0].eventType").value("ADD"))
                .andExpect(jsonPath("$[0].book.bookId").value(1))
                .andExpect(jsonPath("$[0].book.bookName").value("Clean Code"))
                .andExpect(jsonPath("$[0].book.bookAuthor").value("Robert C. Martin"))
                .andExpect(jsonPath("$[0].createdAt").isNotEmpty())
                .andExpect(jsonPath("$[0].updatedAt").isNotEmpty());
    }

    @Test
    void getAllLibraryEvents_withoutBook_shouldReturnNullBook() throws Exception {
        LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
        libraryEventRepository.save(libraryEvent);

        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$[0].eventType").value("ADD"))
                .andExpect(jsonPath("$[0].book").isEmpty());
    }

    // ── GET by ID ────────────────────────────────────────────

    @Test
    void getLibraryEventById_shouldReturnLibraryEvent() throws Exception {
        LibraryEvent savedEvent = persistLibraryEventWithBook(1L, "Clean Code", "Robert C. Martin");

        mockMvc.perform(get("/v1/library-events/{id}", savedEvent.getLibraryEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(savedEvent.getLibraryEventId()))
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(1))
                .andExpect(jsonPath("$.book.bookName").value("Clean Code"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Robert C. Martin"))
                .andExpect(jsonPath("$.book.libraryEventId").value(savedEvent.getLibraryEventId()))
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void getLibraryEventById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/library-events/999"))
                .andExpect(status().isNotFound());
    }

    @Test
    void getLibraryEventById_withoutBook_shouldReturnNullBook() throws Exception {
        LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.UPDATE, null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        mockMvc.perform(get("/v1/library-events/{id}", savedEvent.getLibraryEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(savedEvent.getLibraryEventId()))
                .andExpect(jsonPath("$.eventType").value("UPDATE"))
                .andExpect(jsonPath("$.book").isEmpty());
    }

    // ── Helper ───────────────────────────────────────────────

    private LibraryEvent persistLibraryEventWithBook(@jakarta.validation.constraints.NotNull Long bookId, String bookName, String bookAuthor) {
        LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        Book book = new Book(bookId, bookName, bookAuthor);
        book.setLibraryEvent(savedEvent);
        bookRepository.save(book);

        return savedEvent;
    }
}


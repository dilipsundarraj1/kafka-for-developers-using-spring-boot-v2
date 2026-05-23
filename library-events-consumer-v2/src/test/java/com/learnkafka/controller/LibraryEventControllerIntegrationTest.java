package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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

    @Test
    void getAllLibraryEvents_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAllLibraryEvents_shouldReturnAllLibraryEvents() throws Exception {
        persistLibraryEventWithBook(100, "Clean Code", "Robert C. Martin", LibraryEventType.ADD);
        persistLibraryEventWithBook(101, "Kafka in Action", "John Doe", LibraryEventType.UPDATE);

        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void getLibraryEventById_shouldReturnLibraryEvent() throws Exception {
        LibraryEvent savedEvent = persistLibraryEventWithBook(200, "Domain-Driven Design", "Eric Evans", LibraryEventType.ADD);

        mockMvc.perform(get("/v1/library-events/{libraryEventId}", savedEvent.getLibraryEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(savedEvent.getLibraryEventId()))
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(200))
                .andExpect(jsonPath("$.book.bookName").value("Domain-Driven Design"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Eric Evans"))
                .andExpect(jsonPath("$.book.libraryEventId").value(savedEvent.getLibraryEventId()))
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty())
                .andExpect(jsonPath("$.book.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.book.updatedAt").isNotEmpty());
    }

    @Test
    void getLibraryEventById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/library-events/{libraryEventId}", 999))
                .andExpect(status().isNotFound());
    }

    private LibraryEvent persistLibraryEventWithBook(Integer bookId,
                                                     String bookName,
                                                     String bookAuthor,
                                                     LibraryEventType eventType) {
        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setEventType(eventType);

        Book book = new Book();
        book.setBookId(bookId);
        book.setBookName(bookName);
        book.setBookAuthor(bookAuthor);

        libraryEvent.setBook(book);
        return libraryEventRepository.save(libraryEvent);
    }
}


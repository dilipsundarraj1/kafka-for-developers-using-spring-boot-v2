package com.learnkafka.controller;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.BookDto;
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
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ImportTestcontainers
class BookControllerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    @Test
    void getAllBooks_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAllBooks_shouldReturnAllBooks() throws Exception {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");
        persistBookWithLibraryEvent(2, "Effective Java", "Joshua Bloch");

        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void getBookById_shouldReturnBook() throws Exception {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");

        mockMvc.perform(get("/v1/books/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId").value(1))
                .andExpect(jsonPath("$.bookName").value("Clean Code"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"))
                .andExpect(jsonPath("$.libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void getBookById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/books/999"))
                .andExpect(status().isNotFound());
    }

    @Test
    void createBook_shouldPersistAndReturn201() throws Exception {
        BookDto bookDto = new BookDto(10, "Domain-Driven Design", "Eric Evans");

        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(bookDto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.bookId").value(10))
                .andExpect(jsonPath("$.bookName").value("Domain-Driven Design"))
                .andExpect(jsonPath("$.bookAuthor").value("Eric Evans"))
                .andExpect(jsonPath("$.libraryEventId").isEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void createBook_invalidPayload_shouldReturn400() throws Exception {
        BookDto bookDto = new BookDto(null, "", "");

        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(bookDto)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void updateBook_shouldUpdateAndReturn200() throws Exception {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");
        BookDto updateDto = new BookDto(1, "Clean Code 2nd Edition", "Robert C. Martin");

        mockMvc.perform(put("/v1/books/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookName").value("Clean Code 2nd Edition"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"));
    }

    @Test
    void updateBook_notFound_shouldReturn404() throws Exception {
        BookDto updateDto = new BookDto(999, "Non-existent", "Nobody");

        mockMvc.perform(put("/v1/books/999")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isNotFound());
    }

    @Test
    void deleteBook_shouldDeleteAndReturn204() throws Exception {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");

        mockMvc.perform(delete("/v1/books/1"))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteBook_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(delete("/v1/books/999"))
                .andExpect(status().isNotFound());
    }

    private void persistBookWithLibraryEvent(Integer bookId, String bookName, String bookAuthor) {
        LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        Book book = new Book(bookId, bookName, bookAuthor);
        book.setLibraryEvent(savedEvent);
        bookRepository.save(book);
    }
}

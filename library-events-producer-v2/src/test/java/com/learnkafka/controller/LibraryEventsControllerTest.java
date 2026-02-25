package com.learnkafka.controller;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private Book validBook;
    private LibraryEvent validAddEvent;

    @BeforeEach
    void setUp() {
        validBook = new Book(1, "Kafka in Action", "John Doe");
        validAddEvent = new LibraryEvent(null, LibraryEventType.ADD, validBook);
    }

    @Test
    @DisplayName("POST should return 201 Created with valid ADD event")
    void testPostLibraryEventWithValidAddEvent() throws Exception {
        // Given
        String requestBody = objectMapper.writeValueAsString(validAddEvent);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isCreated())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.libraryEventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(1))
                .andExpect(jsonPath("$.book.bookName").value("Kafka in Action"))
                .andExpect(jsonPath("$.book.bookAuthor").value("John Doe"));
    }

    @Test
    @DisplayName("POST should reject UPDATE event type")
    void testPostLibraryEventWithUpdateTypeShouldFail() throws Exception {
        // Given
        LibraryEvent updateEvent = new LibraryEvent(1, LibraryEventType.UPDATE, validBook);
        String requestBody = objectMapper.writeValueAsString(updateEvent);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with null libraryEventType")
    void testPostLibraryEventWithNullEventTypeShouldFail() throws Exception {
        // Given
        LibraryEvent nullTypeEvent = new LibraryEvent(null, null, validBook);
        String requestBody = objectMapper.writeValueAsString(nullTypeEvent);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with null book")
    void testPostLibraryEventWithNullBookShouldFail() throws Exception {
        // Given
        LibraryEvent nullBookEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
        String requestBody = objectMapper.writeValueAsString(nullBookEvent);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with null book name")
    void testPostLibraryEventWithNullBookNameShouldFail() throws Exception {
        // Given
        Book invalidBook = new Book(1, null, "John Doe");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with blank book name")
    void testPostLibraryEventWithBlankBookNameShouldFail() throws Exception {
        // Given
        Book invalidBook = new Book(1, "", "John Doe");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with null book author")
    void testPostLibraryEventWithNullBookAuthorShouldFail() throws Exception {
        // Given
        Book invalidBook = new Book(1, "Kafka in Action", null);
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with blank book author")
    void testPostLibraryEventWithBlankBookAuthorShouldFail() throws Exception {
        // Given
        Book invalidBook = new Book(1, "Kafka in Action", "");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should reject event with null book ID")
    void testPostLibraryEventWithNullBookIdShouldFail() throws Exception {
        // Given
        Book invalidBook = new Book(null, "Kafka in Action", "John Doe");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST should accept event with libraryEventId=null for ADD type")
    void testPostLibraryEventWithNullLibraryEventIdForAddShouldSucceed() throws Exception {
        // Given
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, validBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isCreated());
    }
}

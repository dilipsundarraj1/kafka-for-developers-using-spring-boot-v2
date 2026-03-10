package com.learnkafka.controller;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for LibraryEventsController using Embedded Kafka.
 * This test suite verifies the controller's behavior with real Kafka instance.
 * No mocking is used in these tests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = "library-events"
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "library.events.topic=library-events"
})
class LibraryEventsControllerIntegrationTest {

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
    @DisplayName("Integration: POST should return 201 Created with valid ADD event and publish to Kafka")
    void testPostLibraryEventWithValidAddEvent_ShouldPublishToKafka() throws Exception {
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
    @DisplayName("Integration: POST with different valid book should succeed")
    void testPostLibraryEventWithDifferentValidBook_ShouldSucceed() throws Exception {
        // Given
        Book differentBook = new Book(2, "Spring in Action", "Craig Walls");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, differentBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.book.bookId").value(2))
                .andExpect(jsonPath("$.book.bookName").value("Spring in Action"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Craig Walls"));
    }

    @Test
    @DisplayName("Integration: POST should reject UPDATE event type")
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
    @DisplayName("Integration: POST should reject event with null libraryEventType")
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
    @DisplayName("Integration: POST should reject event with null book")
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
    @DisplayName("Integration: POST should reject event with null book ID")
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
    @DisplayName("Integration: POST should reject event with null book name")
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
    @DisplayName("Integration: POST should reject event with blank book name")
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
    @DisplayName("Integration: POST should reject event with null book author")
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
    @DisplayName("Integration: POST should reject event with blank book author")
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
    @DisplayName("Integration: POST should accept event with libraryEventId=null for ADD type")
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

    @Test
    @DisplayName("Integration: POST with multiple valid events should all succeed")
    void testMultiplePostLibraryEventsWithValidData_AllShouldSucceed() throws Exception {
        // Given
        Book book1 = new Book(10, "Clean Code", "Robert C. Martin");
        Book book2 = new Book(11, "Effective Java", "Joshua Bloch");
        Book book3 = new Book(12, "Design Patterns", "Gang of Four");

        LibraryEvent event1 = new LibraryEvent(null, LibraryEventType.ADD, book1);
        LibraryEvent event2 = new LibraryEvent(null, LibraryEventType.ADD, book2);
        LibraryEvent event3 = new LibraryEvent(null, LibraryEventType.ADD, book3);

        // When & Then - First event
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event1)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.book.bookName").value("Clean Code"));

        // When & Then - Second event
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event2)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.book.bookName").value("Effective Java"));

        // When & Then - Third event
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event3)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.book.bookName").value("Design Patterns"));
    }

    @Test
    @DisplayName("Integration: POST with invalid JSON should return 400 Bad Request")
    void testPostLibraryEventWithInvalidJsonShouldFail() throws Exception {
        // Given
        String invalidJson = "{invalid json}";

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: POST without Content-Type header should fail")
    void testPostLibraryEventWithoutContentTypeShouldFail() throws Exception {
        // Given
        String requestBody = objectMapper.writeValueAsString(validAddEvent);

        // When & Then
        mockMvc.perform(post("/v1/library-events")
                .content(requestBody))
                .andExpect(status().isUnsupportedMediaType());
    }

    // ==================== PUT /v1/library-events/{libraryEventId} Tests ====================

    @Test
    @DisplayName("Integration: PUT should return 202 Accepted with valid UPDATE event and publish to Kafka")
    void testPutLibraryEventWithValidUpdateEvent_ShouldReturn202() throws Exception {
        // Given
        LibraryEvent updateEvent = new LibraryEvent(1, LibraryEventType.UPDATE, validBook);
        String requestBody = objectMapper.writeValueAsString(updateEvent);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.libraryEventId").value(1))
                .andExpect(jsonPath("$.libraryEventType").value("UPDATE"))
                .andExpect(jsonPath("$.book.bookId").value(1))
                .andExpect(jsonPath("$.book.bookName").value("Kafka in Action"))
                .andExpect(jsonPath("$.book.bookAuthor").value("John Doe"));
    }

    @Test
    @DisplayName("Integration: PUT should use path variable libraryEventId, overriding body value")
    void testPutLibraryEvent_PathVariableOverridesBodyId() throws Exception {
        // Given - body has libraryEventId=999, but path has 42
        LibraryEvent updateEvent = new LibraryEvent(999, LibraryEventType.UPDATE, validBook);
        String requestBody = objectMapper.writeValueAsString(updateEvent);

        // When & Then - response should have libraryEventId=42 from path
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 42)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.libraryEventId").value(42));
    }

    @Test
    @DisplayName("Integration: PUT should accept ADD event type (no type restriction on PUT)")
    void testPutLibraryEvent_WithAddEventType_ShouldSucceed() throws Exception {
        // Given - PUT endpoint uses @Valid (default group), not PostValidation group
        LibraryEvent addEvent = new LibraryEvent(null, LibraryEventType.ADD, validBook);
        String requestBody = objectMapper.writeValueAsString(addEvent);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.libraryEventId").value(1))
                .andExpect(jsonPath("$.libraryEventType").value("ADD"));
    }

    @Test
    @DisplayName("Integration: PUT should set libraryEventId from path even when body has null id")
    void testPutLibraryEvent_WithNullBodyId_ShouldUsePathId() throws Exception {
        // Given - use ADD type because UPDATE with null libraryEventId fails bean validation
        // (isLibraryEventIdValidForUpdate) before the controller can set the path variable
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, validBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then - path variable 5 should be used as the libraryEventId in the response
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 5)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.libraryEventId").value(5))
                .andExpect(jsonPath("$.libraryEventType").value("ADD"));
    }

    @Test
    @DisplayName("Integration: PUT should reject event with null libraryEventType")
    void testPutLibraryEvent_WithNullEventType_ShouldReturn400() throws Exception {
        // Given
        LibraryEvent nullTypeEvent = new LibraryEvent(1, null, validBook);
        String requestBody = objectMapper.writeValueAsString(nullTypeEvent);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: PUT should reject event with null book")
    void testPutLibraryEvent_WithNullBook_ShouldReturn400() throws Exception {
        // Given
        LibraryEvent nullBookEvent = new LibraryEvent(1, LibraryEventType.UPDATE, null);
        String requestBody = objectMapper.writeValueAsString(nullBookEvent);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: PUT should reject event with null book ID")
    void testPutLibraryEvent_WithNullBookId_ShouldReturn400() throws Exception {
        // Given
        Book invalidBook = new Book(null, "Kafka in Action", "John Doe");
        LibraryEvent event = new LibraryEvent(1, LibraryEventType.UPDATE, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: PUT should reject event with blank book name")
    void testPutLibraryEvent_WithBlankBookName_ShouldReturn400() throws Exception {
        // Given
        Book invalidBook = new Book(1, "", "John Doe");
        LibraryEvent event = new LibraryEvent(1, LibraryEventType.UPDATE, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: PUT should reject event with blank book author")
    void testPutLibraryEvent_WithBlankBookAuthor_ShouldReturn400() throws Exception {
        // Given
        Book invalidBook = new Book(1, "Kafka in Action", "");
        LibraryEvent event = new LibraryEvent(1, LibraryEventType.UPDATE, invalidBook);
        String requestBody = objectMapper.writeValueAsString(event);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: PUT with invalid JSON should return 400 Bad Request")
    void testPutLibraryEvent_WithInvalidJson_ShouldReturn400() throws Exception {
        // Given
        String invalidJson = "{invalid json}";

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidJson))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Integration: PUT without Content-Type header should return 415 Unsupported Media Type")
    void testPutLibraryEvent_WithoutContentType_ShouldReturn415() throws Exception {
        // Given
        LibraryEvent updateEvent = new LibraryEvent(1, LibraryEventType.UPDATE, validBook);
        String requestBody = objectMapper.writeValueAsString(updateEvent);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 1)
                        .content(requestBody))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    @DisplayName("Integration: PUT should return the complete updated event with all book fields")
    void testPutLibraryEvent_ResponseContainsAllFields() throws Exception {
        // Given
        Book updatedBook = new Book(99, "Updated Book Title", "Updated Author");
        LibraryEvent updateEvent = new LibraryEvent(10, LibraryEventType.UPDATE, updatedBook);
        String requestBody = objectMapper.writeValueAsString(updateEvent);

        // When & Then
        mockMvc.perform(put("/v1/library-events/{libraryEventId}", 10)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.libraryEventId").value(10))
                .andExpect(jsonPath("$.libraryEventType").value("UPDATE"))
                .andExpect(jsonPath("$.book.bookId").value(99))
                .andExpect(jsonPath("$.book.bookName").value("Updated Book Title"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Updated Author"));
    }
}

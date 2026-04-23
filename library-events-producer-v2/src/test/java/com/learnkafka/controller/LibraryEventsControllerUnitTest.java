package com.learnkafka.controller;

import com.learnkafka.exception.LibraryEventPublishException;
import com.learnkafka.service.LibraryEventService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = LibraryEventsController.class)
@Import(LibraryEventsControllerAdvice.class)
class LibraryEventsControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private LibraryEventService libraryEventService;

    @Test
    void postLibraryEvent_returns201_whenEventTypeAdd() throws Exception {
        String requestBody = """
                {
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.createLibraryEvent(any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(0)));

        MvcResult mvcResult = mockMvc.perform(post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(123))
                .andExpect(jsonPath("$.book.bookName").value("Kafka Using Spring Boot"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Dilip"));
    }

    @Test
    void postLibraryEvent_returns400_whenEventTypeIsNotAdd() throws Exception {
        String requestBody = """
                {
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        MvcResult mvcResult = mockMvc.perform(post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value("only ADD event type is supported"));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void postLibraryEvent_returns400_withValidationErrorsEnvelope_whenRequestBodyInvalid() throws Exception {
        String requestBody = """
                {
                  "eventType": "ADD",
                  "book": {
                    "bookName": "",
                    "bookAuthor": ""
                  }
                }
                """;

        mockMvc.perform(post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("bookAuthor is required"))
                .andExpect(jsonPath("$.errors[1]").value("bookId is required"))
                .andExpect(jsonPath("$.errors[2]").value("bookName is required"));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void postLibraryEvent_returns400_withAdviceEnvelope_whenRequestBodyMalformed() throws Exception {
        String requestBody = """
                {
                  "eventType": "DELETE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        mockMvc.perform(post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value(org.hamcrest.Matchers.startsWith("Invalid request body:")));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void postLibraryEvent_returns500_whenServiceThrowsPublishException() throws Exception {
        String requestBody = """
                {
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.createLibraryEvent(any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new LibraryEventPublishException("Failed to publish library event to Kafka", new RuntimeException("Kafka unavailable"))));

        MvcResult mvcResult = mockMvc.perform(post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("Failed to publish library event to Kafka"));
    }

    @Test
    void postLibraryEvent_returns500_withGenericEnvelope_whenServiceThrowsUnexpectedException() throws Exception {
        String requestBody = """
                {
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.createLibraryEvent(any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Unexpected failure")));

        MvcResult mvcResult = mockMvc.perform(post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("An unexpected error occurred. Please try again later."));
    }

    @Test
    void putLibraryEvent_returns200_whenEventTypeUpdateAndIdPresent() throws Exception {
        String requestBody = """
                {
                  "libraryEventId": 123,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.updateLibraryEvent(any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(0)));

        MvcResult mvcResult = mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(123))
                .andExpect(jsonPath("$.eventType").value("UPDATE"))
                .andExpect(jsonPath("$.book.bookId").value(123))
                .andExpect(jsonPath("$.book.bookName").value("Kafka Using Spring Boot"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Dilip"));
    }

    @Test
    void putLibraryEvent_returns400_whenLibraryEventIdMissing() throws Exception {
        String requestBody = """
                {
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        MvcResult mvcResult = mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("libraryEventId is required for UPDATE"));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void putLibraryEvent_returns400_whenEventTypeIsNotUpdate() throws Exception {
        String requestBody = """
                {
                  "libraryEventId": 123,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        MvcResult mvcResult = mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("only UPDATE event type is supported"));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void putLibraryEvent_returns400_withValidationErrorsEnvelope_whenRequestBodyInvalid() throws Exception {
        String requestBody = """
                {
                  "libraryEventId": 123,
                  "eventType": "UPDATE",
                  "book": {
                    "bookName": "",
                    "bookAuthor": ""
                  }
                }
                """;

        mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("bookAuthor is required"))
                .andExpect(jsonPath("$.errors[1]").value("bookId is required"))
                .andExpect(jsonPath("$.errors[2]").value("bookName is required"));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void putLibraryEvent_returns400_withAdviceEnvelope_whenRequestBodyMalformed() throws Exception {
        String requestBody = """
                {
                  "libraryEventId": 123,
                  "eventType": "DELETE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value(org.hamcrest.Matchers.startsWith("Invalid request body:")));

        verifyNoInteractions(libraryEventService);
    }

    @Test
    void putLibraryEvent_returns500_whenServiceThrowsPublishException() throws Exception {
        String requestBody = """
                {
                  "libraryEventId": 123,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.updateLibraryEvent(any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new LibraryEventPublishException("Failed to publish library event to Kafka", new RuntimeException("Kafka unavailable"))));

        MvcResult mvcResult = mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("Failed to publish library event to Kafka"));
    }

    @Test
    void putLibraryEvent_returns500_withGenericEnvelope_whenServiceThrowsUnexpectedException() throws Exception {
        String requestBody = """
                {
                  "libraryEventId": 123,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.updateLibraryEvent(any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Unexpected failure")));

        MvcResult mvcResult = mockMvc.perform(put("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("An unexpected error occurred. Please try again later."));
    }
}





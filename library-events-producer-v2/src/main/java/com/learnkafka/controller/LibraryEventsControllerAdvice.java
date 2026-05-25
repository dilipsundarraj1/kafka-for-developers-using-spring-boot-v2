package com.learnkafka.controller;

import com.learnkafka.exception.LibraryEventPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Global exception handler for the Library Events API.
 *
 * <p>All error responses follow the same shape:
 * <pre>
 * {
 *   "errors": [ "fieldA is required", "fieldB is required" ]
 * }
 * </pre>
 */
@RestControllerAdvice
public class LibraryEventsControllerAdvice {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsControllerAdvice.class);

    // -----------------------------------------------------------------------
    // Bean-validation failures  (@Valid / @Validated)
    // -----------------------------------------------------------------------

    /**
     * Handles constraint violations raised by {@code @Valid} on the request body.
     * Collects every field-level message and returns them sorted so the order is
     * deterministic and easy to assert against in tests.
     *
     * <p>Example response (HTTP 400):
     * <pre>
     * { "errors": ["bookAuthor is required", "bookId is required", "bookName is required"] }
     * </pre>
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(FieldError::getDefaultMessage)
                .filter(msg -> msg != null && !msg.isBlank())
                .sorted()
                .collect(Collectors.toList());

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(errors));
    }

    // -----------------------------------------------------------------------
    // JSON parse / enum deserialisation failures
    // -----------------------------------------------------------------------

    /**
     * Handles malformed JSON bodies or unknown enum values
     * (e.g. {@code "eventType": "DELETE"} when only ADD / UPDATE exist).
     *
     * <p>Example response (HTTP 400):
     * <pre>
     * { "errors": ["Invalid request body: Cannot deserialise value of type ..."] }
     * </pre>
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMessageNotReadable(HttpMessageNotReadableException ex) {
        String detail = ex.getMostSpecificCause().getMessage();
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(List.of("Invalid request body: " + detail)));
    }

    // -----------------------------------------------------------------------
    // Kafka publish failures
    // -----------------------------------------------------------------------

    /**
     * Handles failures when the Kafka producer cannot publish the event.
     *
     * <p>Example response (HTTP 500):
     * <pre>
     * { "errors": ["Failed to publish library event to Kafka"] }
     * </pre>
     */
    @ExceptionHandler(LibraryEventPublishException.class)
    public ResponseEntity<ErrorResponse> handlePublishException(LibraryEventPublishException ex) {
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(List.of(ex.getMessage())));
    }

    // -----------------------------------------------------------------------
    // Static-resource / favicon 404s
    // -----------------------------------------------------------------------

    /**
     * Handles requests for static resources that do not exist (e.g. {@code /favicon.ico}).
     * Returns a plain 404 without logging an ERROR, so browser-initiated requests do not
     * pollute the application error log.
     */
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<Void> handleNoResourceFound(NoResourceFoundException ex) {
        log.debug("Static resource not found: {}", ex.getMessage());
        return ResponseEntity.notFound().build();
    }

    // -----------------------------------------------------------------------
    // Catch-all fallback
    // -----------------------------------------------------------------------

    /**
     * Catches any exception not handled by a more specific handler above.
     *
     * <p>Avoids leaking internal stack-trace details to the caller — only a
     * generic message is returned while the full exception is logged.
     *
     * <p>Example response (HTTP 500):
     * <pre>
     * { "errors": ["An unexpected error occurred. Please try again later."] }
     * </pre>
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex) {
        log.error("Unhandled exception: {}", ex.getMessage(), ex);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(List.of("An unexpected error occurred. Please try again later.")));
    }

    // -----------------------------------------------------------------------
    // Response envelope
    // -----------------------------------------------------------------------

    /**
     * Uniform error response envelope returned for every error case.
     *
     * @param errors one or more human-readable error messages
     */
    public record ErrorResponse(List<String> errors) {}
}



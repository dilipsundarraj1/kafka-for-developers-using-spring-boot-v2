package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.validation.PostValidation;
import com.learnkafka.producer.LibraryEventProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/library-events")
@Validated
@Tag(name = "Library Events", description = "Publish library events to Kafka")
public class LibraryEventsController {
    private static final Logger logger = LoggerFactory.getLogger(LibraryEventsController.class);
    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventsController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @Operation(
            summary = "Create a new library event",
            description = "Publishes a new ADD event to the Kafka 'library-events' topic. " +
                    "The libraryEventType must be ADD."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Event published successfully",
                    content = @Content(schema = @Schema(implementation = LibraryEvent.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request body or validation failure",
                    content = @Content)
    })
    @PostMapping
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @Validated(PostValidation.class) @RequestBody @Valid LibraryEvent libraryEvent) {
        logger.info("Received POST request to create library event: {}", libraryEvent);
        logger.debug("Library Event Type: {}, Book: {}", libraryEvent.libraryEventType(), libraryEvent.book());

        libraryEventProducer.sendLibraryEvent(libraryEvent);

        logger.info("Library event created successfully: {}", libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @Operation(
            summary = "Update an existing library event",
            description = "Publishes an UPDATE event to the Kafka 'library-events' topic. " +
                    "The libraryEventId from the path is used as the Kafka message key."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Event accepted and published",
                    content = @Content(schema = @Schema(implementation = LibraryEvent.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request body or validation failure",
                    content = @Content)
    })
    @PutMapping("/{libraryEventId}")
    public ResponseEntity<LibraryEvent> putLibraryEvent(
            @Parameter(description = "ID of the library event to update", required = true)
            @PathVariable Integer libraryEventId,
            @RequestBody @Valid LibraryEvent libraryEvent) {
        LibraryEvent libraryEventForUpdate = new LibraryEvent(
                libraryEventId,
                libraryEvent.libraryEventType(),
                libraryEvent.book()
        );

        logger.info("Received PUT request to update library event id {}: {}", libraryEventId, libraryEventForUpdate);
        libraryEventProducer.sendLibraryEvent(libraryEventForUpdate);
        logger.info("Library event updated successfully: {}", libraryEventForUpdate);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(libraryEventForUpdate);
    }
}

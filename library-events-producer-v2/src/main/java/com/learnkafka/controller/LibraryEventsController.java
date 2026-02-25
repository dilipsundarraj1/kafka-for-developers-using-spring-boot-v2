package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.validation.PostValidation;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/library-events")
@Validated
public class LibraryEventsController {
    private static final Logger logger = LoggerFactory.getLogger(LibraryEventsController.class);

    /**
     * POST /v1/library-events - Create a new library event
     *
     * @param libraryEvent the library event to create (must have libraryEventType = ADD)
     * @return 201 Created with the created event
     */
    @PostMapping
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @Validated(PostValidation.class) @RequestBody @Valid LibraryEvent libraryEvent) {
        logger.info("Received POST request to create library event: {}", libraryEvent);

        // Log the details of the received event
        logger.debug("Library Event Type: {}, Book: {}", libraryEvent.libraryEventType(), libraryEvent.book());

        // TODO: Publish event to Kafka
        logger.info("Library event created successfully: {}", libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}

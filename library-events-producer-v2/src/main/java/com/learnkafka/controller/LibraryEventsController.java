package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.validation.PostValidation;
import com.learnkafka.producer.LibraryEventProducer;
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
public class LibraryEventsController {
    private static final Logger logger = LoggerFactory.getLogger(LibraryEventsController.class);
    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventsController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

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

        libraryEventProducer.sendLibraryEvent(libraryEvent);

        // TODO: Publish event to Kafka
        logger.info("Library event created successfully: {}", libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    /**
     * PUT /v1/library-events/{libraryEventId} - Update an existing library event
     *
     * @param libraryEventId the event id from path, used as the Kafka key
     * @param libraryEvent the incoming library event payload
     * @return 202 Accepted with the updated event payload
     */
    @PutMapping("/{libraryEventId}")
    public ResponseEntity<LibraryEvent> putLibraryEvent(@PathVariable Integer libraryEventId,
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

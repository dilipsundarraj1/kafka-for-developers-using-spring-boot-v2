package com.learnkafka.service;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.exception.LibraryEventPublishException;
import com.learnkafka.producer.LibraryEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service layer for {@link LibraryEvent} operations.
 *
 * <p>Enforces business rules before delegating to the Kafka producer.
 * Keeps the controller thin: validation (Bean Validation) lives on the
 * controller, while business-rule enforcement lives here.
 */
@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventService(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    /**
     * Publishes a new library event to Kafka asynchronously.
     *
     * <p>The caller (controller) is responsible for enforcing {@code eventType == ADD}
     * before invoking this method.
     *
     * @param libraryEvent the validated event from the controller
     * @return a {@link CompletableFuture} that completes with the same event once the
     *         broker acknowledges the message, or exceptionally with a
     *         {@link LibraryEventPublishException} on failure
     */
    public CompletableFuture<LibraryEvent> createLibraryEvent(LibraryEvent libraryEvent) {

        log.debug("Creating library event: libraryEventId={}, bookId={}",
                libraryEvent.libraryEventId(),
                libraryEvent.book() != null ? libraryEvent.book().bookId() : null);

        return libraryEventProducer
               // .sendLibraryEvent(libraryEvent)
                .sendLibraryEventWithTransactionalAnnotation(libraryEvent)
                .thenApply(_ -> libraryEvent)
                .exceptionally(ex -> {
                    throw new LibraryEventPublishException(
                            "Failed to publish library event to Kafka", ex.getCause());
                });
    }

    /**
     * Publishes an updated library event to Kafka asynchronously.
     *
     * <p>The caller (controller) is responsible for enforcing {@code eventType == UPDATE}
     * and a non-null {@code libraryEventId} before invoking this method.
     *
     * @param libraryEvent the validated event from the controller
     * @return a {@link CompletableFuture} that completes with the same event once the
     *         broker acknowledges the message, or exceptionally with a
     *         {@link LibraryEventPublishException} on failure
     */
    public CompletableFuture<LibraryEvent> updateLibraryEvent(LibraryEvent libraryEvent) {

        log.debug("Updating library event: libraryEventId={}, bookId={}",
                libraryEvent.libraryEventId(),
                libraryEvent.book() != null ? libraryEvent.book().bookId() : null);

        // thenApply  — transforms SendResult<Long, LibraryEvent> → LibraryEvent so the
        //              controller can echo the payload in the 200 OK body.
        //              (whenComplete in the producer already logs partition / offset.)
        //
        // exceptionally — re-wraps the raw Kafka exception into LibraryEventPublishException
        //                 so LibraryEventsControllerAdvice.handlePublishException fires
        //                 correctly for async controller results.
        //                 (whenComplete already logged the failure at the transport layer.)
        return libraryEventProducer.sendLibraryEvent(libraryEvent)
                .thenApply(_ -> libraryEvent)
                .exceptionally(ex -> {
                    throw new LibraryEventPublishException(
                            "Failed to publish library event to Kafka", ex.getCause());
                });
    }
}


package com.learnjava.controller;

import com.learnjava.config.AppConstants;
import com.learnjava.controller.LibraryEventsControllerAdvice.ErrorResponse;
import com.learnjava.domain.LibraryEvent;
import com.learnjava.domain.LibraryEventType;
import com.learnjava.service.LibraryEventService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping(AppConstants.API_BASE_PATH)
public class LibraryEventsController {

    private final LibraryEventService libraryEventService;

    public LibraryEventsController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<?>> createLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) {

        if (libraryEvent.eventType() != LibraryEventType.ADD) {
            return CompletableFuture.completedFuture(
                    ResponseEntity
                            .status(HttpStatus.BAD_REQUEST)
                            .body(new ErrorResponse(List.of("only ADD event type is supported"))));
        }

        return libraryEventService.createLibraryEvent(libraryEvent)
                .thenApply(created -> ResponseEntity
                        .status(HttpStatus.CREATED)
                        .body(created));
    }

    /**
     * Handles PUT requests for updating an existing library event.
     *
     * <p>Enforces:
     * <ul>
     *   <li>{@code libraryEventId} must be non-null (identifies the record to update).</li>
     *   <li>{@code eventType} must be {@link LibraryEventType#UPDATE}.</li>
     * </ul>
     *
     * <p>The publish is performed asynchronously via
     * {@link LibraryEventService#updateLibraryEvent(LibraryEvent)}.
     * Spring MVC resolves the returned {@link CompletableFuture} without blocking
     * the request-handling thread.
     *
     * @param libraryEvent the validated update event from the request body
     * @return {@code 200 OK} with the full event payload once published,
     *         or {@code 400 Bad Request} if business rules are violated
     */
    @PutMapping
    public CompletableFuture<ResponseEntity<?>> updateLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) {

        if (libraryEvent.libraryEventId() == null) {
            return CompletableFuture.completedFuture(
                    ResponseEntity
                            .status(HttpStatus.BAD_REQUEST)
                            .body(new ErrorResponse(List.of("libraryEventId is required for UPDATE"))));
        }

        if (libraryEvent.eventType() != LibraryEventType.UPDATE) {
            return CompletableFuture.completedFuture(
                    ResponseEntity
                            .status(HttpStatus.BAD_REQUEST)
                            .body(new ErrorResponse(List.of("only UPDATE event type is supported"))));
        }

        return libraryEventService.updateLibraryEvent(libraryEvent)
                .thenApply(updated -> ResponseEntity.ok().body(updated));
    }
}


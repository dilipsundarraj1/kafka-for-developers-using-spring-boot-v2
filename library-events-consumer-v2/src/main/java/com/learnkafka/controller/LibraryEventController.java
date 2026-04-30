package com.learnkafka.controller;

import com.learnkafka.dto.LibraryEventResponseDto;
import com.learnkafka.service.LibraryEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/library-events")
public class LibraryEventController {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventController.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @GetMapping
    public ResponseEntity<List<LibraryEventResponseDto>> getAllLibraryEvents() {
        log.info("GET /v1/library-events");
        List<LibraryEventResponseDto> libraryEvents = libraryEventService.findAll();
        return ResponseEntity.ok(libraryEvents);
    }

    @GetMapping("/{libraryEventId}")
    public ResponseEntity<LibraryEventResponseDto> getLibraryEventById(
            @PathVariable Long libraryEventId) {
        log.info("GET /v1/library-events/{}", libraryEventId);
        return libraryEventService.findById(libraryEventId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}


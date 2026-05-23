package com.learnkafka.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEventDTO(
        Long libraryEventId,

        @NotNull(message = "eventType is required")
        LibraryEventType eventType,

        @Valid
        @NotNull(message = "book is required")
        Book book
) {
}

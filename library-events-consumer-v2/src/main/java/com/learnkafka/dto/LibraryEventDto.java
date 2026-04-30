package com.learnkafka.dto;

import com.learnkafka.domain.LibraryEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEventDto(
        Long libraryEventId,

        @NotNull
        LibraryEventType eventType,

        @NotNull
        @Valid
        BookDto book
) {
}


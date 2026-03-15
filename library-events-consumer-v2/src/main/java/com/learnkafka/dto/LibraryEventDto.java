package com.learnkafka.dto;

import com.learnkafka.domain.LibraryEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEventDto(
        Integer libraryEventId,

        @NotNull
        LibraryEventType libraryEventType,

        @NotNull
        @Valid
        BookDto book
) {
}


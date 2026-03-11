package com.learnkafka.dto;

import com.learnkafka.domain.EventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEventDto(
        Integer libraryEventId,

        @NotNull
        EventType eventType,

        @NotNull
        @Valid
        BookDto book
) {
}


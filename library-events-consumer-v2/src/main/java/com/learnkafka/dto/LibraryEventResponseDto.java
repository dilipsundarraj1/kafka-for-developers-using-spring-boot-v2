package com.learnkafka.dto;

import com.learnkafka.domain.LibraryEventType;

import java.time.LocalDateTime;

public record LibraryEventResponseDto(
        Long libraryEventId,
        LibraryEventType eventType,
        BookResponseDto book,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}


package com.learnkafka.dto;

import com.learnkafka.domain.LibraryEventType;

import java.time.LocalDateTime;

public record LibraryEventResponseDto(
        Integer libraryEventId,
        LibraryEventType eventType,
        BookResponseDto book,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}


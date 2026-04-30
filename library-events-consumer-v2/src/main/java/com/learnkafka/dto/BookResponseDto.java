package com.learnkafka.dto;

import java.time.LocalDateTime;

public record BookResponseDto(
        Long bookId,
        String bookName,
        String bookAuthor,
        Long libraryEventId,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}


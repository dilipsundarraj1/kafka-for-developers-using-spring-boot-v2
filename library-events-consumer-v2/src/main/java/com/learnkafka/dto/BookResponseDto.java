package com.learnkafka.dto;

import java.time.LocalDateTime;

public record BookResponseDto(
        Integer bookId,
        String bookName,
        String bookAuthor,
        Integer libraryEventId,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}


package com.learnkafka.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record Book(
        @NotNull(message = "bookId is required")
        Long bookId,

        @NotBlank(message = "bookName is required")
        String bookName,

        @NotBlank(message = "bookAuthor is required")
        String bookAuthor
) {
}

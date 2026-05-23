package com.learnkafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record BookDto(
        @NotNull Integer bookId,
        @NotBlank String bookName,
        @NotBlank String bookAuthor
) {
}


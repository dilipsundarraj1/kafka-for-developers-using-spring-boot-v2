package com.learnkafka.dto;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

public class LibraryEventMapper {

    private LibraryEventMapper() {
    }

    public static LibraryEvent toEntity(LibraryEventDto dto) {
        Book book = toBookEntity(dto.book());
        return new LibraryEvent(dto.libraryEventId(), dto.libraryEventType(), book);
    }

    public static Book toBookEntity(BookDto dto) {
        return new Book(dto.bookId(), dto.bookName(), dto.bookAuthor());
    }
}



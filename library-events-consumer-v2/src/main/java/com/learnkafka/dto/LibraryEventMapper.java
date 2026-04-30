package com.learnkafka.dto;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

public class LibraryEventMapper {

    private LibraryEventMapper() {
    }

    public static LibraryEvent toEntity(LibraryEventDto dto) {
        Book book = toBookEntity(dto.book());
        return new LibraryEvent(dto.libraryEventId(), dto.eventType(), book);
    }

    public static Book toBookEntity(BookDto dto) {
        return new Book(dto.bookId(), dto.bookName(), dto.bookAuthor());
    }

    public static BookResponseDto toBookResponseDto(Book book) {
        Long libraryEventId = book.getLibraryEvent() != null
                ? book.getLibraryEvent().getLibraryEventId()
                : null;
        return new BookResponseDto(
                book.getBookId(),
                book.getBookName(),
                book.getBookAuthor(),
                libraryEventId,
                book.getCreatedAt(),
                book.getUpdatedAt()
        );
    }

    public static LibraryEventResponseDto toLibraryEventResponseDto(LibraryEvent libraryEvent) {
        BookResponseDto bookResponseDto = libraryEvent.getBook() != null
                ? toBookResponseDto(libraryEvent.getBook())
                : null;
        return new LibraryEventResponseDto(
                libraryEvent.getLibraryEventId(),
                libraryEvent.getEventType(),
                bookResponseDto,
                libraryEvent.getCreatedAt(),
                libraryEvent.getUpdatedAt()
        );
    }
}



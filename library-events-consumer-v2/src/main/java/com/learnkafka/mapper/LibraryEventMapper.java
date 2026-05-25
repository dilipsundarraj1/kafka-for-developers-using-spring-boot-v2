package com.learnkafka.mapper;

import com.learnkafka.domain.LibraryEventDTO;
import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.BookResponseDto;
import com.learnkafka.dto.LibraryEventResponseDto;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventMapper {

    public LibraryEvent toEntity(LibraryEventDTO dto) {
        validateBook(dto);

        LibraryEvent libraryEvent = new LibraryEvent();
        libraryEvent.setEventType(dto.eventType());

        Book book = mapBook(dto);
        libraryEvent.setBook(book);

        return libraryEvent;
    }

    public void updateEntity(LibraryEventDTO dto, LibraryEvent existing) {
        validateBook(dto);

        existing.setEventType(dto.eventType());

        Book existingBook = existing.getBook();
        if (existingBook == null) {
            existingBook = new Book();
        }

        existingBook.setBookId(toBookId(dto.book().bookId()));
        existingBook.setBookName(dto.book().bookName());
        existingBook.setBookAuthor(dto.book().bookAuthor());
        existing.setBook(existingBook);
    }

    public Book toBookEntity(BookDto dto) {
        Book book = new Book();
        book.setBookId(dto.bookId());
        book.setBookName(dto.bookName());
        book.setBookAuthor(dto.bookAuthor());
        return book;
    }

    public BookResponseDto toBookResponseDto(Book book) {
        Integer libraryEventId = book.getLibraryEvent() != null
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

    public LibraryEventResponseDto toLibraryEventResponseDto(LibraryEvent libraryEvent) {
        BookResponseDto book = libraryEvent.getBook() != null
                ? toBookResponseDto(libraryEvent.getBook())
                : null;
        return new LibraryEventResponseDto(
                libraryEvent.getLibraryEventId(),
                libraryEvent.getEventType(),
                book,
                libraryEvent.getCreatedAt(),
                libraryEvent.getUpdatedAt()
        );
    }

    private Book mapBook(LibraryEventDTO dto) {
        Book book = new Book();
        book.setBookId(toBookId(dto.book().bookId()));
        book.setBookName(dto.book().bookName());
        book.setBookAuthor(dto.book().bookAuthor());
        return book;
    }

    private void validateBook(LibraryEventDTO dto) {
        if (dto.book() == null) {
            throw new IllegalArgumentException("book is required");
        }
    }

    private Integer toBookId(Long id) {
        if (id == null) {
            throw new IllegalArgumentException("book.bookId is required");
        }
        return Math.toIntExact(id);
    }
}

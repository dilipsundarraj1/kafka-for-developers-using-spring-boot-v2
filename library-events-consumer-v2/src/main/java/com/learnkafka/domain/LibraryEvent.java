package com.learnkafka.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.validation.constraints.NotNull;

@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    @NotNull
    private LibraryEventType eventType;

    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    private Book book;

    public LibraryEvent() {
    }

    public LibraryEvent(Integer libraryEventId, LibraryEventType eventType, Book book) {
        this.libraryEventId = libraryEventId;
        this.eventType = eventType;
        this.book = book;
    }

    public Integer getLibraryEventId() {
        return libraryEventId;
    }

    public void setLibraryEventId(Integer libraryEventId) {
        this.libraryEventId = libraryEventId;
    }

    public LibraryEventType getEventType() {
        return eventType;
    }

    public void setEventType(LibraryEventType eventType) {
        this.eventType = eventType;
    }

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }

    @Override
    public String toString() {
        return "LibraryEvent{" +
                "libraryEventId=" + libraryEventId +
                ", eventType=" + eventType +
                ", book=" + book +
                '}';
    }
}


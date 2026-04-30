package com.learnkafka.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;

@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long libraryEventId;

    @Enumerated(EnumType.STRING)
    @NotNull
    private LibraryEventType eventType;

    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    private Book book;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public LibraryEvent() {
    }

    public LibraryEvent(Long libraryEventId, LibraryEventType eventType, Book book) {
        this.libraryEventId = libraryEventId;
        this.eventType = eventType;
        this.book = book;
    }

    public Long getLibraryEventId() {
        return libraryEventId;
    }

    public void setLibraryEventId(Long libraryEventId) {
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

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "LibraryEvent{" +
                "libraryEventId=" + libraryEventId +
                ", eventType=" + eventType +
                ", book=" + book +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}

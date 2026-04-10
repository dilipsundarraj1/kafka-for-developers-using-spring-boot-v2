package com.learnjava.exception;

/**
 * Thrown when the Kafka producer fails to publish a {@code LibraryEvent}.
 * Wraps the underlying transport/serialization cause so the caller
 * can choose to expose a clean 5xx without leaking internal stack details.
 */
public class LibraryEventPublishException extends RuntimeException {

    public LibraryEventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}


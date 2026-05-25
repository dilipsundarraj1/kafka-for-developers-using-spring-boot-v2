CREATE TABLE library_event (
    library_event_id SERIAL PRIMARY KEY,
    event_type       VARCHAR(255) NOT NULL
);

CREATE TABLE book (
    book_id          INTEGER      PRIMARY KEY,
    book_name        VARCHAR(255) NOT NULL,
    book_author      VARCHAR(255) NOT NULL,
    library_event_id INTEGER,
    CONSTRAINT fk_book_library_event
        FOREIGN KEY (library_event_id)
        REFERENCES library_event (library_event_id)
);


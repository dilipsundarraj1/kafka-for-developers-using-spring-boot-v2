CREATE TABLE failure_record (
    id          SERIAL PRIMARY KEY,
    topic       VARCHAR(255)  NOT NULL,
    key_value   INTEGER,
    error_record TEXT         NOT NULL,
    partition   INTEGER       NOT NULL,
    offset_value BIGINT       NOT NULL,
    exception   TEXT          NOT NULL,
    status      VARCHAR(10)   NOT NULL,   -- OPEN | FIXED
    created_at  TIMESTAMP     NOT NULL,
    updated_at  TIMESTAMP     NOT NULL
);

CREATE INDEX idx_failure_record_status ON failure_record (status);

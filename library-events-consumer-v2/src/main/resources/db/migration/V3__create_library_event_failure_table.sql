CREATE TABLE library_event_failure (
    failure_id         BIGSERIAL PRIMARY KEY,
    topic              VARCHAR(255) NOT NULL,
    partition_id       INTEGER      NOT NULL,
    offset_value       BIGINT       NOT NULL,
    record_key         VARCHAR(255),
    payload            TEXT         NOT NULL,
    exception_class    VARCHAR(500) NOT NULL,
    exception_message  TEXT,
    stack_trace        TEXT         NOT NULL,
    failed_at          TIMESTAMP    NOT NULL DEFAULT now()
);


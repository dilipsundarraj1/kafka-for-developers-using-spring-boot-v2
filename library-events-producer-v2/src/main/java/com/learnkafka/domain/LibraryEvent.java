package com.learnkafka.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.learnkafka.domain.validation.PostValidation;
import com.learnkafka.domain.validation.PutValidation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record LibraryEvent(
    @NotNull(groups = PutValidation.class) Integer libraryEventId,
    @NotNull LibraryEventType libraryEventType,
    @NotNull @Valid Book book
) {
    private static final Logger logger = LoggerFactory.getLogger(LibraryEvent.class);

    @JsonIgnore
    @AssertTrue(message = "libraryEventType must be ADD for POST", groups = PostValidation.class)
    public boolean isPostLibraryEventTypeValid() {
        logger.debug("Validating POST Library Event Type. Current type: {}", libraryEventType);
        if (libraryEventType == null) {
            logger.warn("Library event type is null during POST validation");
            return true;
        }
        boolean isValid = libraryEventType == LibraryEventType.ADD;
        if (!isValid) {
            logger.error("Invalid library event type for POST: expected ADD but got {}", libraryEventType);
        } else {
            logger.info("POST Library Event Type validation passed");
        }
        return isValid;
    }

    @JsonIgnore
    @AssertTrue(message = "libraryEventId is required when libraryEventType is UPDATE")
    public boolean isLibraryEventIdValidForUpdate() {
        logger.debug("Validating Library Event ID for UPDATE. Event type: {}, Event ID: {}", libraryEventType, libraryEventId);
        if (libraryEventType == null) {
            logger.warn("Library event type is null during UPDATE validation");
            return true;
        }
        boolean isValid = libraryEventType != LibraryEventType.UPDATE || libraryEventId != null;
        if (!isValid) {
            logger.error("Invalid update: libraryEventId is required when libraryEventType is UPDATE");
        } else {
            logger.info("Library Event ID validation passed for update");
        }
        return isValid;
    }
}

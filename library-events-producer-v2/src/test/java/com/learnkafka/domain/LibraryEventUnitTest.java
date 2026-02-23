package com.learnkafka.domain;

import com.learnkafka.domain.validation.PostValidation;
import com.learnkafka.domain.validation.PutValidation;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.groups.Default;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LibraryEventUnitTest {

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void closeValidator() {
        validatorFactory.close();
    }

    @Test
    void postWithAddAndNoIdIsValid() {
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, validBook());

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event, PostValidation.class);

        assertTrue(violations.isEmpty());
    }

    @Test
    void postWithUpdateTypeIsRejected() {
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.UPDATE, validBook());

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event, PostValidation.class);
        IO.println("violations : " + violations);

        assertEquals(1, violations.size());
        assertTrue(violations.iterator().next().getMessage().contains("libraryEventType must be ADD for POST"));
    }

    @Test
    void putWithUpdateTypeAndMissingIdIsRejected() {
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.UPDATE, validBook());

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event, PutValidation.class, Default.class);

        assertEquals(2, violations.size());
    }

    @Test
    void putWithUpdateTypeAndIdIsValid() {
        LibraryEvent event = new LibraryEvent(10, LibraryEventType.UPDATE, validBook());

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event, PutValidation.class, Default.class);

        assertTrue(violations.isEmpty());
    }

    @Test
    void bookFieldsAreRequired() {
        Book book = new Book(100, " ", "");
        LibraryEvent event = new LibraryEvent(1, LibraryEventType.ADD, book);

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);

        assertEquals(2, violations.size());
    }

    @Test
    void libraryEventTypeIsRequired() {
        LibraryEvent event = new LibraryEvent(1, null, validBook());

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);

        assertEquals(1, violations.size());
        assertTrue(violations.iterator().next().getMessage().contains("must not be null"));
    }

    private static Book validBook() {
        return new Book(100, "Clean Code", "Robert C. Martin");
    }
}


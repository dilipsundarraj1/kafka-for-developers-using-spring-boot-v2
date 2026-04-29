package com.learnkafka.producer;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.exception.LibraryEventPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

// StringSerializer mode (switch): uncomment the two imports below
// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes {@link LibraryEvent} messages to a Kafka topic.
 *
 * <p>Topic is read from {@code spring.kafka.topic} in {@code application.yml}.
 * The message key is {@code libraryEventId} so that events for the same
 * library record land on the same partition (ordering guarantee).
 *
 * <p>A {@code whenComplete} callback is attached to every send for
 * success/failure logging without blocking the calling thread.
 * The returned {@link CompletableFuture} can be blocked on by the
 * caller when a synchronous guarantee is required.
 *
 * <p><b>Serializer mode (JacksonJsonSerializer — active):</b>
 * {@code KafkaTemplate} serializes {@code LibraryEvent} automatically via Jackson's
 * {@code JacksonJsonSerializer}. To switch to {@code StringSerializer}, see the commented
 * code in this class and toggle {@code application.yml}.
 */
@Component
public class LibraryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventProducer.class);

    @Value("${spring.kafka.topic}")
    private String topic;

    // JacksonJsonSerializer mode: KafkaTemplate carries the LibraryEvent object directly
    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;
    // StringSerializer mode (switch): swap the line above with the one below
    // private final KafkaTemplate<Long, String> kafkaTemplate;

    // StringSerializer mode (switch): add ObjectMapper field below
    // private final ObjectMapper objectMapper;

    public LibraryEventProducer(KafkaTemplate<Long, LibraryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    // StringSerializer mode (switch): replace constructor above with:
    // public LibraryEventProducer(KafkaTemplate<Long, String> kafkaTemplate, ObjectMapper objectMapper) {
    //     this.kafkaTemplate = kafkaTemplate;
    //     this.objectMapper  = objectMapper;
    // }

    /**
     * Publishes {@code libraryEvent} to the configured Kafka topic.
     *
     * @param libraryEvent the event to publish; its {@code libraryEventId} is used as the message key
     * @return a {@link CompletableFuture} that completes with the send result or
     *         exceptionally with a {@link LibraryEventPublishException}
     */
    public CompletableFuture<SendResult<Long, LibraryEvent>> sendLibraryEvent(LibraryEvent libraryEvent) {
        // StringSerializer mode (switch): change return type to CompletableFuture<SendResult<Long, String>>
        //                                 and replace kafkaTemplate.send(...) call below with the
        //                                 manual serialization block:
        //   String value;
        //   try {
        //       value = objectMapper.writeValueAsString(libraryEvent);
        //   } catch (JsonProcessingException e) {
        //       throw new LibraryEventPublishException("Failed to serialize LibraryEvent to JSON", e);
        //   }
        //   CompletableFuture<SendResult<Long, String>> future = kafkaTemplate.send(topic, key, value);

        Long key = libraryEvent.libraryEventId();

        log.info("Sending LibraryEvent to topic={}, key={}, eventType={}", topic, key, libraryEvent.eventType());

        CompletableFuture<SendResult<Long, LibraryEvent>> future = kafkaTemplate.send(topic, key, libraryEvent);

        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish LibraryEvent | topic={}, key={}, error={}",
                        topic, key, ex.getMessage(), ex);
            } else {
                var metadata = result.getRecordMetadata();
                log.info("Published LibraryEvent | topic={}, partition={}, offset={}, key={}",
                        metadata.topic(), metadata.partition(), metadata.offset(), key);
            }
        });
    }

    /**
     * Publishes {@code libraryEvent} to the configured Kafka topic <em>synchronously</em>.
     *
     * <p>Unlike {@link #sendLibraryEvent(LibraryEvent)}, this method blocks the calling
     * thread until the broker acknowledgement is received (or a timeout/error occurs).
     * Use this when you need a guaranteed delivery confirmation before continuing.
     *
     * @param libraryEvent the event to publish; its {@code libraryEventId} is used as the message key
     * @return the {@link SendResult} containing broker metadata for the published record
     * @throws LibraryEventPublishException if the send fails, times out, or the thread is interrupted
     */
    public SendResult<Long, LibraryEvent> sendLibraryEventSynchronous(LibraryEvent libraryEvent) {
        Long key = libraryEvent.libraryEventId();

        log.info("Sending LibraryEvent synchronously to topic={}, key={}, eventType={}",
                topic, key, libraryEvent.eventType());

        try {
            SendResult<Long, LibraryEvent> result = kafkaTemplate.send(topic, key, libraryEvent)
                    .get(3, TimeUnit.SECONDS);

            var metadata = result.getRecordMetadata();
            log.info("Published LibraryEvent synchronously | topic={}, partition={}, offset={}, key={}",
                    metadata.topic(), metadata.partition(), metadata.offset(), key);

            return result;
        } catch (ExecutionException ex) {
            log.error("Failed to publish LibraryEvent synchronously | topic={}, key={}, error={}",
                    topic, key, ex.getMessage(), ex);
            throw new LibraryEventPublishException("Failed to publish LibraryEvent synchronously", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing LibraryEvent synchronously | topic={}, key={}", topic, key);
            throw new LibraryEventPublishException("Interrupted while publishing LibraryEvent synchronously", ex);
        } catch (TimeoutException ex) {
            log.error("Timed out while publishing LibraryEvent synchronously | topic={}, key={}", topic, key);
            throw new LibraryEventPublishException("Timed out while publishing LibraryEvent synchronously", ex);
        }
    }

    /**
     * Publishes {@code libraryEvent} asynchronously inside a Spring-managed Kafka transaction.
     *
     * <p>This method mirrors {@link #sendLibraryEvent(LibraryEvent)} behavior but uses
     * {@link Transactional} instead of {@code executeInTransaction(...)}.
     */
    @Transactional("kafkaTransactionManager")
    public CompletableFuture<SendResult<Long, LibraryEvent>> sendLibraryEventWithTransactionalAnnotation(
            LibraryEvent libraryEvent) {
        Long key = libraryEvent.libraryEventId();

        log.info("Sending LibraryEvent with @Transactional (async) to topic={}, key={}, eventType={}",
                topic, key, libraryEvent.eventType());

        if (isAbortSimulationBook(libraryEvent)) {
            CompletableFuture<SendResult<Long, LibraryEvent>> first = kafkaTemplate.send(topic, key, libraryEvent);
            CompletableFuture<SendResult<Long, LibraryEvent>> second = kafkaTemplate.send(topic, key, libraryEvent);
            CompletableFuture<SendResult<Long, LibraryEvent>> third = kafkaTemplate.send(topic, key, libraryEvent);

            CompletableFuture.allOf(first, second, third).join();
            throw new LibraryEventPublishException(
                    "Simulated transaction abort after publishing 3 duplicate events for bookName=transaction",
                    new IllegalStateException("Simulated abort trigger"));
        }

        CompletableFuture<SendResult<Long, LibraryEvent>> future = kafkaTemplate.send(topic, key, libraryEvent);

        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish LibraryEvent with @Transactional (async) | topic={}, key={}, error={}",
                        topic, key, ex.getMessage(), ex);
            } else {
                var metadata = result.getRecordMetadata();
                log.info("Published LibraryEvent with @Transactional (async) | topic={}, partition={}, offset={}, key={}",
                        metadata.topic(), metadata.partition(), metadata.offset(), key);
            }
        });
    }

    /**
     * Publishes {@code libraryEvent} synchronously inside a Spring-managed Kafka transaction.
     */
    @Transactional("kafkaTransactionManager")
    public SendResult<Long, LibraryEvent> sendLibraryEventSynchronousWithTransactionalAnnotation(
            LibraryEvent libraryEvent) {
        Long key = libraryEvent.libraryEventId();

        log.info("Sending LibraryEvent with @Transactional (sync) to topic={}, key={}, eventType={}",
                topic, key, libraryEvent.eventType());

        if (isAbortSimulationBook(libraryEvent)) {
            try {
                kafkaTemplate.send(topic, key, libraryEvent).get(3, TimeUnit.SECONDS);
                kafkaTemplate.send(topic, key, libraryEvent).get(3, TimeUnit.SECONDS);
                kafkaTemplate.send(topic, key, libraryEvent).get(3, TimeUnit.SECONDS);
                throw new LibraryEventPublishException(
                        "Simulated transaction abort after publishing 3 duplicate events for bookName=transaction",
                        new IllegalStateException("Simulated abort trigger"));
            } catch (ExecutionException ex) {
                throw new LibraryEventPublishException("Failed during simulated transaction abort sequence", ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new LibraryEventPublishException("Interrupted during simulated transaction abort sequence", ex);
            } catch (TimeoutException ex) {
                throw new LibraryEventPublishException("Timed out during simulated transaction abort sequence", ex);
            }
        }

        try {
            SendResult<Long, LibraryEvent> result = kafkaTemplate.send(topic, key, libraryEvent)
                    .get(3, TimeUnit.SECONDS);

            var metadata = result.getRecordMetadata();
            log.info("Published LibraryEvent with @Transactional (sync) | topic={}, partition={}, offset={}, key={}",
                    metadata.topic(), metadata.partition(), metadata.offset(), key);

            return result;
        } catch (ExecutionException ex) {
            log.error("Failed to publish LibraryEvent with @Transactional (sync) | topic={}, key={}, error={}",
                    topic, key, ex.getMessage(), ex);
            throw new LibraryEventPublishException("Failed to publish LibraryEvent with @Transactional (sync)", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing LibraryEvent with @Transactional (sync) | topic={}, key={}", topic, key);
            throw new LibraryEventPublishException("Interrupted while publishing LibraryEvent with @Transactional (sync)", ex);
        } catch (TimeoutException ex) {
            log.error("Timed out while publishing LibraryEvent with @Transactional (sync) | topic={}, key={}", topic, key);
            throw new LibraryEventPublishException("Timed out while publishing LibraryEvent with @Transactional (sync)", ex);
        }
    }

    /**
     * Publishes {@code libraryEvent} transactionally using an async send flow.
     *
     * <p>This mirrors {@link #sendLibraryEvent(LibraryEvent)} (async callback style),
     * but executes the send inside {@link KafkaTemplate#executeInTransaction}.
     *
     * @param libraryEvent the event to publish transactionally
     * @return a {@link CompletableFuture} for the send result
     */
    public CompletableFuture<SendResult<Long, LibraryEvent>> sendLibraryEventTransactionalAsync(
            LibraryEvent libraryEvent) {
        Long key = libraryEvent.libraryEventId();

        log.info("Sending LibraryEvent transactionally (async) to topic={}, key={}, eventType={}",
                topic, key, libraryEvent.eventType());

        try {
            return kafkaTemplate.executeInTransaction(operations -> {
                if (isAbortSimulationBook(libraryEvent)) {
                    CompletableFuture<SendResult<Long, LibraryEvent>> first = operations.send(topic, key, libraryEvent);
                    CompletableFuture<SendResult<Long, LibraryEvent>> second = operations.send(topic, key, libraryEvent);
                    CompletableFuture<SendResult<Long, LibraryEvent>> third = operations.send(topic, key, libraryEvent);

                    CompletableFuture.allOf(first, second, third).join();
                    throw new LibraryEventPublishException(
                            "Simulated transaction abort after publishing 3 duplicate events for bookName=transaction",
                            new IllegalStateException("Simulated abort trigger"));
                }

                CompletableFuture<SendResult<Long, LibraryEvent>> future = operations.send(topic, key, libraryEvent);

                return future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish LibraryEvent transactionally (async) | topic={}, key={}, error={}",
                                topic, key, ex.getMessage(), ex);
                    } else {
                        var metadata = result.getRecordMetadata();
                        log.info("Published LibraryEvent transactionally (async) | topic={}, partition={}, offset={}, key={}",
                                metadata.topic(), metadata.partition(), metadata.offset(), key);
                    }
                });
            });
        } catch (RuntimeException ex) {
            log.error("Failed to start transactional async publish | topic={}, key={}, error={}",
                    topic, key, ex.getMessage(), ex);
            CompletableFuture<SendResult<Long, LibraryEvent>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new LibraryEventPublishException(
                    "Failed to publish LibraryEvent transactionally (async)", ex));
            return failed;
        }
    }

    /**
     * Publishes {@code libraryEvent} to Kafka using a producer transaction.
     *
     * <p>This method is intentionally separate from existing send methods.
     * It wraps the send call in {@link KafkaTemplate#executeInTransaction} so
     * the record is produced as part of a Kafka transaction boundary.
     *
     * @param libraryEvent the event to publish transactionally
     * @return the broker send result for the published record
     * @throws LibraryEventPublishException if the transactional send fails
     */
    public SendResult<Long, LibraryEvent> sendLibraryEventTransactional(LibraryEvent libraryEvent) {
        Long key = libraryEvent.libraryEventId();

        log.info("Sending LibraryEvent transactionally to topic={}, key={}, eventType={}",
                topic, key, libraryEvent.eventType());

        try {
            return kafkaTemplate.executeInTransaction(operations -> {
                if (isAbortSimulationBook(libraryEvent)) {
                    try {
                        operations.send(topic, key, libraryEvent).get(3, TimeUnit.SECONDS);
                        operations.send(topic, key, libraryEvent).get(3, TimeUnit.SECONDS);
                        operations.send(topic, key, libraryEvent).get(3, TimeUnit.SECONDS);
                        throw new LibraryEventPublishException(
                                "Simulated transaction abort after publishing 3 duplicate events for bookName=transaction",
                                new IllegalStateException("Simulated abort trigger"));
                    } catch (ExecutionException ex) {
                        throw new LibraryEventPublishException("Failed during simulated transaction abort sequence", ex);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new LibraryEventPublishException("Interrupted during simulated transaction abort sequence", ex);
                    } catch (TimeoutException ex) {
                        throw new LibraryEventPublishException("Timed out during simulated transaction abort sequence", ex);
                    }
                }

                try {
                    SendResult<Long, LibraryEvent> result = operations.send(topic, key, libraryEvent)
                            .get(3, TimeUnit.SECONDS);

                    var metadata = result.getRecordMetadata();
                    log.info("Published LibraryEvent transactionally | topic={}, partition={}, offset={}, key={}",
                            metadata.topic(), metadata.partition(), metadata.offset(), key);

                    return result;
                } catch (ExecutionException ex) {
                    throw new LibraryEventPublishException("Failed to publish LibraryEvent transactionally", ex);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new LibraryEventPublishException("Interrupted while publishing LibraryEvent transactionally", ex);
                } catch (TimeoutException ex) {
                    throw new LibraryEventPublishException("Timed out while publishing LibraryEvent transactionally", ex);
                }
            });
        } catch (RuntimeException ex) {
            if (ex instanceof LibraryEventPublishException) {
                throw ex;
            }

            log.error("Failed to publish LibraryEvent transactionally | topic={}, key={}, error={}",
                    topic, key, ex.getMessage(), ex);
            throw new LibraryEventPublishException("Failed to publish LibraryEvent transactionally", ex);
        }
    }

    private boolean isAbortSimulationBook(LibraryEvent libraryEvent) {
        return libraryEvent != null
                && libraryEvent.book() != null
                && "transaction".equalsIgnoreCase(libraryEvent.book().bookName());
    }
}


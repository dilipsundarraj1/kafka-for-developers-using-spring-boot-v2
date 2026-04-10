package com.learnjava.producer;

import com.learnjava.domain.LibraryEvent;
import com.learnjava.exception.LibraryEventPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

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
 * <p><b>Serializer mode (JsonSerializer — active):</b>
 * {@code KafkaTemplate} serializes {@code LibraryEvent} automatically via Jackson's
 * {@code JsonSerializer}. To switch to {@code StringSerializer}, see the commented
 * code in this class and toggle {@code application.yml}.
 */
@Component
public class LibraryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventProducer.class);

    @Value("${spring.kafka.topic}")
    private String topic;

    // JsonSerializer mode: KafkaTemplate carries the LibraryEvent object directly
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
}


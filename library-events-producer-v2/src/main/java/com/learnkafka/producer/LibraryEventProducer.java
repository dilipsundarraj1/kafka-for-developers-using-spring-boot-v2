package com.learnkafka.producer;

import com.learnkafka.domain.LibraryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class LibraryEventProducer {
    private static final Logger logger = LoggerFactory.getLogger(LibraryEventProducer.class);

    private final KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;
    private final String topicName;

    public LibraryEventProducer(
            KafkaTemplate<Integer, LibraryEvent> kafkaTemplate,
            @Value("${library.events.topic:library-events}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public CompletableFuture<SendResult<Integer, LibraryEvent>> sendLibraryEvent(LibraryEvent libraryEvent) {
        Integer key = libraryEvent.libraryEventId();
        CompletableFuture<SendResult<Integer, LibraryEvent>> future =
                key == null
                        ? kafkaTemplate.send(topicName, libraryEvent)
                        : kafkaTemplate.send(topicName, key, libraryEvent);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("Failed to publish library event. key={} event={}", key, libraryEvent, ex);
                return;
            }
            logger.info(
                    "Published library event. topic={} partition={} offset={} key={} event={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    key,
                    libraryEvent);
        });

        return future;
    }
}


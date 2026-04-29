package com.learnkafka.consumer;

import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventsConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    // BATCH ack mode (default) — offsets are committed automatically after all records
    // from a poll() batch are processed. No Acknowledgment parameter needed.
    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord) {
        log.info("ConsumerRecord : {}", consumerRecord);
        libraryEventService.processEvent(consumerRecord);
    }
}


package com.learnkafka.consumer;

import com.learnkafka.domain.LibraryEventDTO;
import com.learnkafka.service.LibraryEventService;
import jakarta.transaction.Transactional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsConsumer {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);
	private final LibraryEventService libraryEventService;

	public LibraryEventsConsumer(LibraryEventService libraryEventService) {
		this.libraryEventService = libraryEventService;
	}

	// Test -> Event to Kafka topic "library-events" -> onMessage -> libraryEventService.processEvent -> DB
			// Check the DB and the event is persisted
	// Test for ADD, UPDATE
	// Test for Invalid Events
	// Pass the book as null
	// Update Event Type with null libraryEventId



	@Transactional
	@KafkaListener(topics = "library-events")
	public void onMessage(ConsumerRecord<Integer, LibraryEventDTO> consumerRecord
	//					  ,Acknowledgment acknowledgment
	) {
		log.info(
				"Received Kafka record. topic={}, partition={}, offset={}, key={}, value={}",
				consumerRecord.topic(),
				consumerRecord.partition(),
				consumerRecord.offset(),
				consumerRecord.key(),
				consumerRecord.value());
		libraryEventService.processEvent(consumerRecord);
	//	acknowledgment.acknowledge();
	}
}



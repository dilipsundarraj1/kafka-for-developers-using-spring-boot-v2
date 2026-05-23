package com.learnkafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEventDTO;
import com.learnkafka.entity.LibraryEventFailure;
import com.learnkafka.repository.LibraryEventFailureRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.io.PrintWriter;
import java.io.StringWriter;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);
	private static final String DLT_TOPIC = "library-events.DLT";

	@Value("${app.kafka.recovery.mode:failure-table}")
	private String recoveryMode;

	@Bean
	ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

	@Bean
	ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDTO> kafkaListenerContainerFactory(
			ConsumerFactory<Integer, LibraryEventDTO> consumerFactory,
			DefaultErrorHandler defaultErrorHandler) {
		var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDTO>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(defaultErrorHandler);
		//factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // Default ContainerProperties.AckMode.BATCH
//		factory.setConcurrency(3);  // ← 3 consumer threads
		return factory;
	}

	@Bean
	DefaultErrorHandler defaultErrorHandler(KafkaTemplate<Integer, Object> kafkaTemplate,
			LibraryEventFailureRepository libraryEventFailureRepository,
			ObjectMapper objectMapper) {
		var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
		exponentialBackOff.setInitialInterval(1000L);
		exponentialBackOff.setMultiplier(2.0);
		exponentialBackOff.setMaxInterval(4000L);

		String mode = recoveryMode == null ? "" : recoveryMode.trim().toLowerCase();
		DefaultErrorHandler errorHandler = switch (mode) {
			case "dlt" -> {
				var recoverer = new DeadLetterPublishingRecoverer(
						kafkaTemplate,
						(record, exception) -> resolveDltPartition(record, kafkaTemplate)
				);
				log.info("Kafka recovery mode is '{}'; failed records will be published to topic '{}'.", recoveryMode, DLT_TOPIC);
				yield new DefaultErrorHandler(recoverer, exponentialBackOff);
			}
			case "failure-table" -> {
				ConsumerRecordRecoverer failureTableRecoverer = (record, exception) -> {
					var failure = new LibraryEventFailure();
					failure.setTopic(record.topic());
					failure.setPartitionId(record.partition());
					failure.setOffsetValue(record.offset());
					failure.setRecordKey(record.key() != null ? record.key().toString() : null);
					failure.setPayload(toPayload(record, objectMapper));
					failure.setExceptionClass(exception.getClass().getName());
					failure.setExceptionMessage(exception.getMessage());
					failure.setStackTrace(toStackTrace(exception));
					libraryEventFailureRepository.save(failure);
					log.error("Recovery: persisted failed record to table. topic={}, partition={}, offset={}, exceptionType={}",
							record.topic(), record.partition(), record.offset(), exception.getClass().getSimpleName(), exception);
				};
				log.info("Kafka recovery mode is '{}'; failed records will be persisted to the failure table.", recoveryMode);
				yield new DefaultErrorHandler(failureTableRecoverer, exponentialBackOff);
			}
			case "both" -> {
				var dltRecoverer = new DeadLetterPublishingRecoverer(
						kafkaTemplate,
						(record, exception) -> resolveDltPartition(record, kafkaTemplate)
				);
				ConsumerRecordRecoverer bothRecoverer = (record, exception) -> {
					// 1. Publish to DLT
					try{
						dltRecoverer.accept(record, exception);
					}catch (Exception e){
						log.error("Failed to publish to DLT for record. topic={}, partition={}, offset={}, exceptionType={}. Exception: {}",
								record.topic(), record.partition(), record.offset(), exception.getClass().getSimpleName(), e.getMessage(), e);
					}

					// 2. Persist to failure table
					var failure = new LibraryEventFailure();
					failure.setTopic(record.topic());
					failure.setPartitionId(record.partition());
					failure.setOffsetValue(record.offset());
					failure.setRecordKey(record.key() != null ? record.key().toString() : null);
					failure.setPayload(toPayload(record, objectMapper));
					failure.setExceptionClass(exception.getClass().getName());
					failure.setExceptionMessage(exception.getMessage());
					failure.setStackTrace(toStackTrace(exception));
					libraryEventFailureRepository.save(failure);
					log.error("Recovery (both): published to DLT '{}' and persisted to failure table. topic={}, partition={}, offset={}, exceptionType={}",
							DLT_TOPIC, record.topic(), record.partition(), record.offset(), exception.getClass().getSimpleName(), exception);
				};
				log.info("Kafka recovery mode is '{}'; failed records will be published to DLT '{}' AND persisted to the failure table.", recoveryMode, DLT_TOPIC);
				yield new DefaultErrorHandler(bothRecoverer, exponentialBackOff);
			}
			case "log_skip" -> {
				ConsumerRecordRecoverer logAndSkip = (record, exception) -> {
					log.error("Recovery: skipping failed record. Topic={}, Partition={}, Offset={}, Exception={}",
							record.topic(), record.partition(), record.offset(), exception.getMessage(), exception);
				};
				yield new DefaultErrorHandler(logAndSkip, exponentialBackOff);
			}
			default -> {
				log.info("Kafka recovery mode is '{}'; DLT publish is disabled.", recoveryMode);
				yield new DefaultErrorHandler(exponentialBackOff);
			}
		};

		// Previous lambda-based RetryListener (kept for reference):
		// errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
		//     var message = String.format(
		//             "Retry attempt %d failed for topic=%s partition=%d offset=%d: %s",
		//             deliveryAttempt,
		//             record.topic(),
		//             record.partition(),
		//             record.offset(),
		//             ex.getMessage());
		//     log.warn(message, ex);
		// });

		RetryListener retryListener = new RetryListener() {
			@Override
			public void failedDelivery(ConsumerRecord<?, ?> record,
                                       Exception ex,
                                       int deliveryAttempt) {
				var message = String.format(
						"Retry attempt %d failed for topic=%s partition=%d offset=%d: %s",
						deliveryAttempt,
						record.topic(),
						record.partition(),
						record.offset(),
						ex.getMessage());
				log.warn(message, ex);
			}
		};
		errorHandler.addNotRetryableExceptions(
				IllegalArgumentException.class,        // bad payload — will never succeed
				NullPointerException.class,            // programming error
				DataIntegrityViolationException.class  // duplicate key — will always fail
		);
		errorHandler.setRetryListeners(retryListener);

		return errorHandler;
	}

	private TopicPartition resolveDltPartition(ConsumerRecord<?, ?> record, KafkaTemplate<Integer, Object> kafkaTemplate) {
		try {
			var partitions = kafkaTemplate.partitionsFor(DLT_TOPIC);
			int partitionCount = partitions == null ? 0 : partitions.size();
			int partition = partitionCount > 0 ? Math.min(record.partition(), partitionCount - 1) : 0;
			return new TopicPartition(DLT_TOPIC, partition);
		} catch (Exception ex) {
			log.warn("Unable to resolve partition metadata for DLT topic '{}'; defaulting to partition 0.", DLT_TOPIC, ex);
			return new TopicPartition(DLT_TOPIC, 0);
		}
	}

	private String toPayload(ConsumerRecord<?, ?> record, ObjectMapper objectMapper) {
		try {
			return objectMapper.writeValueAsString(record.value());
		} catch (Exception serializationException) {
			log.warn("Failed to serialize consumer record payload. Falling back to String payload representation.", serializationException);
			return String.valueOf(record.value());
		}
	}

	private String toStackTrace(Exception exception) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		exception.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}
}



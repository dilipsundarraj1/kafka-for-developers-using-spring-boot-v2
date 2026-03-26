package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.FailureRecord;
import com.learnkafka.dto.LibraryEventDto;
import com.learnkafka.repository.FailureRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class FailureRecordService {

    private static final Logger log = LoggerFactory.getLogger(FailureRecordService.class);

    public static final String OPEN = "OPEN";
    public static final String FIXED = "FIXED";

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventService libraryEventService;
    private final ObjectMapper objectMapper;

    public FailureRecordService(FailureRecordRepository failureRecordRepository,
                                LibraryEventService libraryEventService,
                                ObjectMapper objectMapper) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventService = libraryEventService;
        this.objectMapper = objectMapper;
    }

    public void saveFailureRecord(ConsumerRecord<Integer, LibraryEventDto> record, Exception exception) {
        String serializedValue;
        try {
            serializedValue = objectMapper.writeValueAsString(record.value());
        } catch (JsonProcessingException e) {
            serializedValue = record.value() != null ? record.value().toString() : "null";
        }

        FailureRecord failureRecord = new FailureRecord(
                record.topic(),
                record.key(),
                serializedValue,
                record.partition(),
                record.offset(),
                exception.getMessage(),
                OPEN
        );

        failureRecordRepository.save(failureRecord);
        log.info("Saved failure record: {}", failureRecord);
    }

    public void retryFailedRecords() {
        List<FailureRecord> openRecords = failureRecordRepository.findAllByStatus(OPEN);
        log.info("Found {} OPEN failure records to retry", openRecords.size());

        openRecords.forEach(failureRecord -> {
            try {
                LibraryEventDto libraryEventDto = objectMapper.readValue(
                        failureRecord.getErrorRecord(), LibraryEventDto.class);

                ConsumerRecord<Integer, LibraryEventDto> consumerRecord =
                        new ConsumerRecord<>(
                                failureRecord.getTopic(),
                                failureRecord.getPartition(),
                                failureRecord.getOffsetValue(),
                                failureRecord.getKeyValue(),
                                libraryEventDto
                        );

                libraryEventService.processEvent(consumerRecord);

                failureRecord.setStatus(FIXED);
                failureRecordRepository.save(failureRecord);
                log.info("Successfully retried failure record id={}, marked as FIXED", failureRecord.getId());

            } catch (Exception e) {
                log.error("Retry failed for failure record id={}: {}", failureRecord.getId(), e.getMessage());
            }
        });
    }
}

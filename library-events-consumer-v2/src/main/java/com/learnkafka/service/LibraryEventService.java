package com.learnkafka.service;

import com.learnkafka.domain.LibraryEventDTO;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.LibraryEventResponseDto;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.mapper.LibraryEventMapper;
import com.learnkafka.repository.LibraryEventRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class LibraryEventService {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);
	private final LibraryEventRepository libraryEventRepository;
	private final LibraryEventMapper libraryEventMapper;
	private final Validator validator;

	public LibraryEventService(
			LibraryEventRepository libraryEventRepository,
			LibraryEventMapper libraryEventMapper,
			Validator validator
	) {
		this.libraryEventRepository = libraryEventRepository;
		this.libraryEventMapper = libraryEventMapper;
		this.validator = validator;
	}

	public List<LibraryEventResponseDto> findAll() {
		log.info("Fetching all library events");
		return libraryEventRepository.findAll()
				.stream()
				.map(libraryEventMapper::toLibraryEventResponseDto)
				.toList();
	}

	public Optional<LibraryEventResponseDto> findById(Integer libraryEventId) {
		log.info("Fetching library event with id: {}", libraryEventId);
		return libraryEventRepository.findById(libraryEventId)
				.map(libraryEventMapper::toLibraryEventResponseDto);
	}

	// Library Event of type ADD
	// Library Event of type UPDATE
	// Library Event of type UPDATE with missing libraryEventId (validation failure)

	public void  processEvent(ConsumerRecord<Integer, LibraryEventDTO> consumerRecord) {
		LibraryEventDTO libraryEventDTO = consumerRecord.value();
		log.info("Received library event DTO: {}", libraryEventDTO);
		try {

			validateDto(libraryEventDTO);
			validateConditionalRules(libraryEventDTO);

			if (libraryEventDTO.eventType() == LibraryEventType.ADD) {
				LibraryEvent libraryEvent = libraryEventMapper.toEntity(libraryEventDTO);
				LibraryEvent saved = libraryEventRepository.save(libraryEvent);
				log.info("Persisted ADD event. libraryEventId={}", saved.getLibraryEventId());
				return;
			}

			if (libraryEventDTO.eventType() == LibraryEventType.UPDATE) {
				Integer libraryEventId = toLibraryEventId(libraryEventDTO.libraryEventId());
				LibraryEvent existing = libraryEventRepository.findById(libraryEventId)
						.orElseThrow(() -> new IllegalArgumentException("LibraryEvent not found for update. libraryEventId=" + libraryEventId));

				libraryEventMapper.updateEntity(libraryEventDTO, existing);
				LibraryEvent updated = libraryEventRepository.save(existing);
				log.info("Persisted UPDATE event. libraryEventId={}", updated.getLibraryEventId());
				return;
			}

			throw new IllegalArgumentException("Unsupported eventType: " + libraryEventDTO.eventType());

		} catch (Exception e) {
			log.error("Error processing library event. consumerRecord={}, error={}", consumerRecord, e.getMessage(), e);
			throw e; // rethrow to trigger DefaultErrorHandler
		}

	}

	private void validateDto(LibraryEventDTO dto) {
		Set<ConstraintViolation<LibraryEventDTO>> violations = validator.validate(dto);
		if (!violations.isEmpty()) {
			String message = violations.stream()
					.map(v -> v.getPropertyPath() + " " + v.getMessage())
					.collect(Collectors.joining(", "));
			log.error("Bean validation failed for library event. dto={}, errors={}", dto, message);
			throw new IllegalArgumentException("Validation failed: " + message);
		}
	}

	private void validateConditionalRules(LibraryEventDTO dto) {
		if (dto.book() == null) {
			log.error("Conditional validation failed: book is null. dto={}", dto);
			throw new IllegalArgumentException("book is required");
		}
		if (dto.eventType() == LibraryEventType.UPDATE && dto.libraryEventId() == null) {
			log.error("Conditional validation failed: UPDATE event missing libraryEventId. dto={}", dto);
			throw new IllegalArgumentException("libraryEventId is required for UPDATE event");
		}
	}

	private Integer toLibraryEventId(Long id) {
		if (id == null) {
			throw new IllegalArgumentException("libraryEventId is required");
		}
		return Math.toIntExact(id);
	}
}

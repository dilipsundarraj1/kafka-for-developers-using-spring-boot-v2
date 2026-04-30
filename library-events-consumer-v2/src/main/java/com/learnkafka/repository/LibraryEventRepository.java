package com.learnkafka.repository;

import com.learnkafka.domain.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventRepository extends JpaRepository<LibraryEvent, Long> {
}


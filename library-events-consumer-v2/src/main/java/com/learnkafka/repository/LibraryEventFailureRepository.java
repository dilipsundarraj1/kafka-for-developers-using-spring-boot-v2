package com.learnkafka.repository;

import com.learnkafka.entity.LibraryEventFailure;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventFailureRepository extends JpaRepository<LibraryEventFailure, Long> {
}


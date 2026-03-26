package com.learnkafka.repository;

import com.learnkafka.domain.FailureRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FailureRecordRepository extends JpaRepository<FailureRecord, Integer> {

    List<FailureRecord> findAllByStatus(String status);
}

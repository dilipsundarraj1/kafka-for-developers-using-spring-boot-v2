package com.learnkafka.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

@Entity
@Table(name = "failure_record")
public class FailureRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false)
    private String topic;

    @Column(name = "key_value")
    private Integer keyValue;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String errorRecord;

    @Column(nullable = false)
    private Integer partition;

    @Column(name = "offset_value", nullable = false)
    private Long offsetValue;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String exception;

    @Column(nullable = false)
    private String status;     // OPEN | FIXED

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    public FailureRecord() {
    }

    public FailureRecord(String topic, Integer keyValue, String errorRecord,
                         Integer partition, Long offsetValue,
                         String exception, String status) {
        this.topic = topic;
        this.keyValue = keyValue;
        this.errorRecord = errorRecord;
        this.partition = partition;
        this.offsetValue = offsetValue;
        this.exception = exception;
        this.status = status;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    public Integer getId() { return id; }

    public String getTopic() { return topic; }

    public Integer getKeyValue() { return keyValue; }

    public String getErrorRecord() { return errorRecord; }

    public Integer getPartition() { return partition; }

    public Long getOffsetValue() { return offsetValue; }

    public String getException() { return exception; }

    public String getStatus() { return status; }

    public void setStatus(String status) {
        this.status = status;
        this.updatedAt = LocalDateTime.now();
    }

    public LocalDateTime getCreatedAt() { return createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }

    @Override
    public String toString() {
        return "FailureRecord{" +
                "id=" + id +
                ", topic='" + topic + '\'' +
                ", keyValue=" + keyValue +
                ", partition=" + partition +
                ", offsetValue=" + offsetValue +
                ", status='" + status + '\'' +
                ", exception='" + exception + '\'' +
                '}';
    }
}

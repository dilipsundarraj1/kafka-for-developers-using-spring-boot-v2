package com.learnkafka.scheduler;

import com.learnkafka.service.FailureRecordService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsScheduler {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsScheduler.class);

    private final FailureRecordService failureRecordService;

    public LibraryEventsScheduler(FailureRecordService failureRecordService) {
        this.failureRecordService = failureRecordService;
    }

    // Runs every 10 seconds — retries all OPEN failure records
    @Scheduled(fixedRateString = "${retry.scheduler.fixed-rate:10000}")
    public void retryFailedRecords() {
        log.info("Scheduler: starting retry of OPEN failure records");
        failureRecordService.retryFailedRecords();
        log.info("Scheduler: completed retry run");
    }
}

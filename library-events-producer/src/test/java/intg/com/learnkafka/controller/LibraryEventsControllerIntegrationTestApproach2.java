package com.learnkafka.controller;

import tools.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ExtendWith(MockitoExtension.class)
public class LibraryEventsControllerIntegrationTestApproach2 {

    @Autowired
    TestRestTemplate restTemplate;

    @MockitoBean
    KafkaTemplate<Integer, String > kafkaTemplate;

    @MockitoBean
    KafkaAdmin kafkaAdmin;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void postLibraryEvent() {


        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        mockProducerCall(libraryEvent, objectMapper.writeValueAsString(libraryEvent));

        //when
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);

        //then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());


        Mockito.verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }


    @Test
    void putLibraryEvent() {
        //given
        var libraryEventUpdate = TestUtil.libraryEventRecordUpdate();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEventUpdate, headers);

        mockProducerCall(libraryEventUpdate, objectMapper.writeValueAsString(libraryEventUpdate));

        //when
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, request, LibraryEvent.class);

        //then
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        Mockito.verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));

    }

    private void mockProducerCall(LibraryEvent libraryEvent, String record) {
        //mock behavior
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.libraryEventId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);
        var future = CompletableFuture.supplyAsync(()-> sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
    }

}
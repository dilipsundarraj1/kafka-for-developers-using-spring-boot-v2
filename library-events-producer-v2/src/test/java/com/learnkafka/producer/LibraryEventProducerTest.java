package com.learnkafka.producer;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.exception.LibraryEventPublishException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @Mock
    private KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    private LibraryEventProducer libraryEventProducer;
    private LibraryEvent libraryEvent;

    @BeforeEach
    void setUp() {
        libraryEventProducer = new LibraryEventProducer(kafkaTemplate);
        ReflectionTestUtils.setField(libraryEventProducer, "topic", "library-events");

        Book book = new Book(123L, "Kafka Using Spring Boot", "Dilip");
        libraryEvent = new LibraryEvent(123L, LibraryEventType.ADD, book);
    }

    @AfterEach
    void clearThreadInterrupt() {
        Thread.interrupted();
    }

    @Test
    void sendLibraryEvent_returnsCompletedFuture_whenKafkaSendSucceeds() {
        SendResult<Long, LibraryEvent> sendResult = mock(SendResult.class);
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(metadata.topic()).thenReturn("library-events");
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(10L);

        when(kafkaTemplate.send("library-events", 123L, libraryEvent))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        CompletableFuture<SendResult<Long, LibraryEvent>> resultFuture =
                libraryEventProducer.sendLibraryEvent(libraryEvent);

        assertThat(resultFuture).isCompleted();
        assertThat(resultFuture.join()).isSameAs(sendResult);
        verify(kafkaTemplate).send("library-events", 123L, libraryEvent);
    }

    @Test
    void sendLibraryEvent_returnsFailedFuture_whenKafkaSendFails() {
        RuntimeException kafkaFailure = new RuntimeException("Kafka unavailable");
        CompletableFuture<SendResult<Long, LibraryEvent>> failedFuture = CompletableFuture.failedFuture(kafkaFailure);

        when(kafkaTemplate.send("library-events", 123L, libraryEvent)).thenReturn(failedFuture);

        CompletableFuture<SendResult<Long, LibraryEvent>> resultFuture =
                libraryEventProducer.sendLibraryEvent(libraryEvent);

        assertThat(resultFuture).isCompletedExceptionally();
        assertThatThrownBy(resultFuture::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(kafkaFailure);
        verify(kafkaTemplate).send("library-events", 123L, libraryEvent);
    }

    @Test
    void sendLibraryEventSynchronous_returnsSendResult_whenKafkaSendSucceeds() {
        CompletableFuture<SendResult<Long, LibraryEvent>> sendFuture = new CompletableFuture<>();
        SendResult<Long, LibraryEvent> sendResult = mock(SendResult.class);
        RecordMetadata metadata = mock(RecordMetadata.class);

        sendFuture.complete(sendResult);
        when(kafkaTemplate.send("library-events", 123L, libraryEvent)).thenReturn(sendFuture);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(metadata.topic()).thenReturn("library-events");
        when(metadata.partition()).thenReturn(1);
        when(metadata.offset()).thenReturn(20L);

        SendResult<Long, LibraryEvent> result = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);

        assertThat(result).isSameAs(sendResult);
        verify(kafkaTemplate).send("library-events", 123L, libraryEvent);
    }

    @Test
    void sendLibraryEventSynchronous_throwsPublishException_whenExecutionExceptionOccurs() throws Exception {
        CompletableFuture<SendResult<Long, LibraryEvent>> sendFuture = new CompletableFuture<>();
        sendFuture.completeExceptionally(new RuntimeException("broker down"));

        when(kafkaTemplate.send("library-events", 123L, libraryEvent)).thenReturn(sendFuture);

        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventSynchronous(libraryEvent))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessage("Failed to publish LibraryEvent synchronously")
                .hasCauseInstanceOf(ExecutionException.class);
    }

    @Test
    void sendLibraryEventSynchronous_throwsPublishExceptionAndInterruptsThread_whenInterruptedExceptionOccurs() throws Exception {
        CompletableFuture<SendResult<Long, LibraryEvent>> sendFuture = org.mockito.Mockito.spy(new CompletableFuture<>());

        when(kafkaTemplate.send("library-events", 123L, libraryEvent)).thenReturn(sendFuture);
        doThrow(new InterruptedException("interrupted")).when(sendFuture).get(3, TimeUnit.SECONDS);

        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventSynchronous(libraryEvent))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessage("Interrupted while publishing LibraryEvent synchronously")
                .hasCauseInstanceOf(InterruptedException.class);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void sendLibraryEventSynchronous_throwsPublishException_whenTimeoutOccurs() throws Exception {
        CompletableFuture<SendResult<Long, LibraryEvent>> sendFuture = org.mockito.Mockito.spy(new CompletableFuture<>());

        when(kafkaTemplate.send("library-events", 123L, libraryEvent)).thenReturn(sendFuture);
        doThrow(new TimeoutException("timeout")).when(sendFuture).get(3, TimeUnit.SECONDS);

        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventSynchronous(libraryEvent))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessage("Timed out while publishing LibraryEvent synchronously")
                .hasCauseInstanceOf(TimeoutException.class);
    }
}

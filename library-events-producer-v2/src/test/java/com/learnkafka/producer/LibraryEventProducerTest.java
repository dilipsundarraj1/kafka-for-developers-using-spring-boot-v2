package com.learnkafka.producer;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    private static final String TOPIC = "library-events";

    @Mock
    private KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    @BeforeEach
    void setUp() {
        // inject the topic name since @Value is not processed by Mockito
        ReflectionTestUtils.setField(libraryEventProducer, "topicName", TOPIC);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SendResult<Integer, LibraryEvent> buildSendResult(Integer key, LibraryEvent event, int partition, long offset) {
        ProducerRecord<Integer, LibraryEvent> producerRecord = new ProducerRecord<>(TOPIC, key, event);
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(TOPIC, partition), offset, 0, 0L, 0, 0);
        return new SendResult<>(producerRecord, recordMetadata);
    }

    // -----------------------------------------------------------------------
    // sendLibraryEvent — async (null key → no-key send)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("sendLibraryEvent: POST (null key) should call kafkaTemplate.send(topic, event)")
    void sendLibraryEvent_withNullKey_shouldSendWithoutKey() {
        // Given
        Book book = new Book(1, "Kafka in Action", "John Doe");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        SendResult<Integer, LibraryEvent> sendResult = buildSendResult(null, event, 0, 10L);
        when(kafkaTemplate.send(eq(TOPIC), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // When
        CompletableFuture<SendResult<Integer, LibraryEvent>> future = libraryEventProducer.sendLibraryEvent(event);

        // Then
        assertThat(future).isNotNull();
        assertThat(future.isDone()).isTrue();
        verify(kafkaTemplate, times(1)).send(TOPIC, event);
        verify(kafkaTemplate, never()).send(eq(TOPIC), anyInt(), any());
    }

    @Test
    @DisplayName("sendLibraryEvent: PUT (non-null key) should call kafkaTemplate.send(topic, key, event)")
    void sendLibraryEvent_withNonNullKey_shouldSendWithKey() {
        // Given
        Book book = new Book(2, "Designing Data-Intensive Apps", "Martin Kleppmann");
        LibraryEvent event = new LibraryEvent(42, LibraryEventType.UPDATE, book);

        SendResult<Integer, LibraryEvent> sendResult = buildSendResult(42, event, 1, 99L);
        when(kafkaTemplate.send(eq(TOPIC), eq(42), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // When
        CompletableFuture<SendResult<Integer, LibraryEvent>> future = libraryEventProducer.sendLibraryEvent(event);

        // Then
        assertThat(future).isNotNull();
        assertThat(future.isDone()).isTrue();
        verify(kafkaTemplate, times(1)).send(TOPIC, 42, event);
        verify(kafkaTemplate, never()).send(eq(TOPIC), eq(event));
    }

    @Test
    @DisplayName("sendLibraryEvent: returned future resolves to correct SendResult metadata")
    void sendLibraryEvent_futureResolvesToCorrectMetadata() throws Exception {
        // Given
        Book book = new Book(3, "Clean Code", "Robert Martin");
        LibraryEvent event = new LibraryEvent(7, LibraryEventType.UPDATE, book);

        SendResult<Integer, LibraryEvent> sendResult = buildSendResult(7, event, 2, 55L);
        when(kafkaTemplate.send(eq(TOPIC), eq(7), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // When
        SendResult<Integer, LibraryEvent> result = libraryEventProducer.sendLibraryEvent(event).get();

        // Then
        assertThat(result.getRecordMetadata().topic()).isEqualTo(TOPIC);
        assertThat(result.getRecordMetadata().partition()).isEqualTo(2);
        assertThat(result.getRecordMetadata().offset()).isEqualTo(55L);
    }

    @Test
    @DisplayName("sendLibraryEvent: future captures correct key in ArgumentCaptor for keyed send")
    void sendLibraryEvent_capturesCorrectKeyViaArgumentCaptor() {
        // Given
        Book book = new Book(4, "Effective Java", "Joshua Bloch");
        LibraryEvent event = new LibraryEvent(100, LibraryEventType.UPDATE, book);

        SendResult<Integer, LibraryEvent> sendResult = buildSendResult(100, event, 0, 1L);
        ArgumentCaptor<Integer> keyCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<LibraryEvent> eventCaptor = ArgumentCaptor.forClass(LibraryEvent.class);

        when(kafkaTemplate.send(eq(TOPIC), keyCaptor.capture(), eventCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // When
        libraryEventProducer.sendLibraryEvent(event);

        // Then
        assertThat(keyCaptor.getValue()).isEqualTo(100);
        assertThat(eventCaptor.getValue().libraryEventType()).isEqualTo(LibraryEventType.UPDATE);
        assertThat(eventCaptor.getValue().book().bookName()).isEqualTo("Effective Java");
    }

    @Test
    @DisplayName("sendLibraryEvent: on Kafka failure, future completes exceptionally")
    void sendLibraryEvent_onKafkaFailure_futureCompletesExceptionally() {
        // Given
        Book book = new Book(5, "The Pragmatic Programmer", "Andrew Hunt");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        CompletableFuture<SendResult<Integer, LibraryEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker unavailable"));
        when(kafkaTemplate.send(eq(TOPIC), eq(event))).thenReturn(failedFuture);

        // When
        CompletableFuture<SendResult<Integer, LibraryEvent>> future = libraryEventProducer.sendLibraryEvent(event);

        // Then
        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Kafka broker unavailable");
    }

    // -----------------------------------------------------------------------
    // sendLibraryEventSynchronous
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("sendLibraryEventSynchronous: null key should call kafkaTemplate.send(topic, event).get()")
    void sendLibraryEventSynchronous_withNullKey_shouldSendWithoutKey() throws Exception {
        // Given
        Book book = new Book(6, "Kafka: The Definitive Guide", "Neha Narkhede");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        SendResult<Integer, LibraryEvent> sendResult = buildSendResult(null, event, 0, 20L);
        when(kafkaTemplate.send(eq(TOPIC), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // When
        SendResult<Integer, LibraryEvent> result = libraryEventProducer.sendLibraryEventSynchronous(event);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo(TOPIC);
        assertThat(result.getRecordMetadata().offset()).isEqualTo(20L);
        verify(kafkaTemplate, times(1)).send(TOPIC, event);
        verify(kafkaTemplate, never()).send(eq(TOPIC), anyInt(), any());
    }

    @Test
    @DisplayName("sendLibraryEventSynchronous: non-null key should call kafkaTemplate.send(topic, key, event).get()")
    void sendLibraryEventSynchronous_withNonNullKey_shouldSendWithKey() throws Exception {
        // Given
        Book book = new Book(7, "Domain-Driven Design", "Eric Evans");
        LibraryEvent event = new LibraryEvent(99, LibraryEventType.UPDATE, book);

        SendResult<Integer, LibraryEvent> sendResult = buildSendResult(99, event, 3, 77L);
        when(kafkaTemplate.send(eq(TOPIC), eq(99), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // When
        SendResult<Integer, LibraryEvent> result = libraryEventProducer.sendLibraryEventSynchronous(event);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().partition()).isEqualTo(3);
        assertThat(result.getRecordMetadata().offset()).isEqualTo(77L);
        verify(kafkaTemplate, times(1)).send(TOPIC, 99, event);
        verify(kafkaTemplate, never()).send(eq(TOPIC), eq(event));
    }

    @Test
    @DisplayName("sendLibraryEventSynchronous: on Kafka failure, should rethrow the exception")
    void sendLibraryEventSynchronous_onKafkaFailure_shouldRethrowException() {
        // Given
        Book book = new Book(8, "Refactoring", "Martin Fowler");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        CompletableFuture<SendResult<Integer, LibraryEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Connection refused"));
        when(kafkaTemplate.send(eq(TOPIC), eq(event))).thenReturn(failedFuture);

        // When / Then
        assertThatThrownBy(() -> libraryEventProducer.sendLibraryEventSynchronous(event))
                .isInstanceOf(Exception.class);
    }

    // -----------------------------------------------------------------------
    // Error classification — Retriable vs Non-Retriable
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("sendLibraryEvent: NetworkException (retriable) — future completes exceptionally and cause is a RetriableException")
    void sendLibraryEvent_withNetworkException_causeIsRetriable() {
        // Given
        Book book = new Book(9, "Kafka: The Definitive Guide", "Neha Narkhede");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        CompletableFuture<SendResult<Integer, LibraryEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new NetworkException("Broker unreachable"));
        when(kafkaTemplate.send(eq(TOPIC), eq(event))).thenReturn(failedFuture);

        // When
        CompletableFuture<SendResult<Integer, LibraryEvent>> future = libraryEventProducer.sendLibraryEvent(event);

        // Then
        assertThat(future.isCompletedExceptionally()).isTrue();
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause())
                .isInstanceOf(NetworkException.class)
                .isInstanceOf(RetriableException.class);  // NetworkException extends RetriableException
    }

    @Test
    @DisplayName("sendLibraryEvent: RecordTooLargeException (non-retriable) — future completes exceptionally and cause is NOT a RetriableException")
    void sendLibraryEvent_withRecordTooLargeException_causeIsNotRetriable() {
        // Given
        Book book = new Book(10, "The Art of Computer Programming", "Donald Knuth");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        CompletableFuture<SendResult<Integer, LibraryEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RecordTooLargeException("Message exceeds max.request.size"));
        when(kafkaTemplate.send(eq(TOPIC), eq(event))).thenReturn(failedFuture);

        // When
        CompletableFuture<SendResult<Integer, LibraryEvent>> future = libraryEventProducer.sendLibraryEvent(event);

        // Then
        assertThat(future.isCompletedExceptionally()).isTrue();
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause())
                .isInstanceOf(RecordTooLargeException.class)
                .isNotInstanceOf(RetriableException.class);  // RecordTooLargeException does NOT extend RetriableException
    }

    @Test
    @DisplayName("sendLibraryEventSynchronous: NetworkException (retriable) — rethrows and cause is a RetriableException")
    void sendLibraryEventSynchronous_withNetworkException_rethrowsRetriableCause() {
        // Given
        Book book = new Book(11, "Building Microservices", "Sam Newman");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        CompletableFuture<SendResult<Integer, LibraryEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new NetworkException("Connection refused"));
        when(kafkaTemplate.send(eq(TOPIC), eq(event))).thenReturn(failedFuture);

        // When
        Throwable thrown = catchThrowable(() -> libraryEventProducer.sendLibraryEventSynchronous(event));

        // Then
        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause())
                .isInstanceOf(NetworkException.class)
                .isInstanceOf(RetriableException.class);
    }

    @Test
    @DisplayName("sendLibraryEventSynchronous: RecordTooLargeException (non-retriable) — rethrows and cause is NOT a RetriableException")
    void sendLibraryEventSynchronous_withRecordTooLargeException_rethrowsNonRetriableCause() {
        // Given
        Book book = new Book(12, "Domain-Driven Design", "Eric Evans");
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, book);

        CompletableFuture<SendResult<Integer, LibraryEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RecordTooLargeException("Record too large"));
        when(kafkaTemplate.send(eq(TOPIC), eq(event))).thenReturn(failedFuture);

        // When
        Throwable thrown = catchThrowable(() -> libraryEventProducer.sendLibraryEventSynchronous(event));

        // Then
        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause())
                .isInstanceOf(RecordTooLargeException.class)
                .isNotInstanceOf(RetriableException.class);
    }
}


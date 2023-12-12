package dev.lydtech.tracking.service;

import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.dispatch.message.TrackingStatusUpdated;
import dev.lydtech.tracking.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class TrackingServiceTest {

    private KafkaTemplate kafkaProducerMock;
    private TrackingService service;

    @BeforeEach
    public void setup() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        service = new TrackingService(kafkaProducerMock);
    }

    @Test
    public void process_Success() throws Exception {
        when(kafkaProducerMock.send( anyString(), anyString(), any(TrackingStatusUpdated.class))).thenReturn(mock(CompletableFuture.class));
        String key = randomUUID().toString();
        DispatchPreparing testEvent = TestEventData.buildDispatchPreparingEvent(UUID.randomUUID());
        service.process(key,testEvent);

        verify(kafkaProducerMock, times(1)).send(eq("tracking.status"), eq(key),any(TrackingStatusUpdated.class));
    }

    @Test
    public void process_DispatchPreparingProducerThrowsException() {
        String key = randomUUID().toString();
        doThrow(new RuntimeException("dispatch preparing producer failure")).when(kafkaProducerMock).send(eq("tracking.status"), eq(key) ,any(TrackingStatusUpdated.class));

        DispatchPreparing testEvent = TestEventData.buildDispatchPreparingEvent(UUID.randomUUID());
        Exception exception = assertThrows(RuntimeException.class, () -> service.process(key,testEvent));

        verify(kafkaProducerMock, times(1)).send(eq("tracking.status"), eq(key),any(TrackingStatusUpdated.class));
        assertThat(exception.getMessage(), equalTo("dispatch preparing producer failure"));
    }
}

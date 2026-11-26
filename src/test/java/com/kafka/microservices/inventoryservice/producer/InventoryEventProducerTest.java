package com.kafka.microservices.inventoryservice.producer;

import com.kafka.microservices.inventoryservice.dto.InventoryResultEvent;
import com.kafka.microservices.inventoryservice.entity.InventoryStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class InventoryEventProducerTest {

    @Mock
    private KafkaTemplate<String, InventoryResultEvent> kafkaTemplate;

    private InventoryEventProducer inventoryEventProducer;

    @BeforeEach
    void setUp() {
        inventoryEventProducer = new InventoryEventProducer(kafkaTemplate);
    }

    @Test
    void publishInventoryResult_shouldPublishInventoryResultToKafka(CapturedOutput output) {
        // Arrange
        UUID orderId = UUID.randomUUID();

        InventoryResultEvent event = new InventoryResultEvent(
                orderId,
                InventoryStatus.AVAILABLE
        );

        when(kafkaTemplate.send("inventory-result", orderId.toString(), event))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        inventoryEventProducer.publishInventoryResult(event);

        // Verify
        verify(kafkaTemplate).send("inventory-result", orderId.toString(), event);

        //Assert
        assertTrue(output.getOut().contains("Published inventory result for order " + orderId));
    }

    @Test
    void publishInventoryResult_shouldLogErrorWhenPublishingFails(CapturedOutput output) {
        // Arrange
        UUID orderId = UUID.randomUUID();

        InventoryResultEvent event = new InventoryResultEvent(
                orderId,
                InventoryStatus.OUT_OF_STOCK
        );

        CompletableFuture<SendResult<String, InventoryResultEvent>> failure = new CompletableFuture<>();

        failure.completeExceptionally(new RuntimeException("Kafka publishing failed"));

        when(kafkaTemplate.send("inventory-result", orderId.toString(), event))
                .thenReturn(failure);

        // Act
        inventoryEventProducer.publishInventoryResult(event);

        // Verify
        verify(kafkaTemplate).send("inventory-result", orderId.toString(), event);

        // Assert
        assertTrue(output.getOut().contains("Failed to publish inventory result for order " + orderId));
        assertTrue(output.getOut().contains("Kafka publishing failed"));
    }
}

package com.kafka.microservices.inventoryservice.service;

import com.kafka.microservices.inventoryservice.dto.InventoryResultEvent;
import com.kafka.microservices.inventoryservice.dto.OrderCreatedEvent;
import com.kafka.microservices.inventoryservice.entity.InventoryStatus;
import com.kafka.microservices.inventoryservice.producer.InventoryEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private InventoryEventProducer inventoryEventProducer;

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService(inventoryEventProducer, 5);
    }

    @Test
    void checkInventory_shouldReturnAvailableWhenQuantityIsWithinStock() {
        // Arrange
        UUID orderId = UUID.randomUUID();

        OrderCreatedEvent orderEvent = new OrderCreatedEvent(
                orderId,
                17L,
                "Phone",
                5,
                LocalDateTime.now()
        );

        ArgumentCaptor<InventoryResultEvent> eventCaptor =
                ArgumentCaptor.forClass(InventoryResultEvent.class);

        // Act
        inventoryService.checkInventory(orderEvent);

        // Verify
        verify(inventoryEventProducer).publishInventoryResult(eventCaptor.capture());

        InventoryResultEvent resultEvent = eventCaptor.getValue();

        // Assert
        assertEquals(orderId, resultEvent.orderId());
        assertEquals(InventoryStatus.AVAILABLE, resultEvent.status());
    }

    @Test
    void checkInventory_shouldReturnOutOfStockWhenQuantityExceedsStock() {
        // Arrange
        UUID orderId = UUID.randomUUID();

        OrderCreatedEvent orderEvent = new OrderCreatedEvent(
                orderId,
                18L,
                "Headphones",
                6,
                LocalDateTime.now()
        );

        ArgumentCaptor<InventoryResultEvent> eventCaptor =
                ArgumentCaptor.forClass(InventoryResultEvent.class);

        // Act
        inventoryService.checkInventory(orderEvent);

        // Verify
        verify(inventoryEventProducer).publishInventoryResult(eventCaptor.capture());

        InventoryResultEvent resultEvent = eventCaptor.getValue();

        // Assert
        assertEquals(orderId, resultEvent.orderId());
        assertEquals(InventoryStatus.OUT_OF_STOCK, resultEvent.status());
    }

    @Test
    void checkInventory_shouldRejectInvalidQuantity() {
        // Arrange
        OrderCreatedEvent orderEvent = new OrderCreatedEvent(
                UUID.randomUUID(),
                19L,
                "Laptop",
                0,
                LocalDateTime.now()
        );

        // Act & Assert
        assertThrows(IllegalArgumentException.class,
                () -> inventoryService.checkInventory(orderEvent));

        // Verify
        verify(inventoryEventProducer, never()).publishInventoryResult(
                org.mockito.ArgumentMatchers.any(InventoryResultEvent.class));
    }
}

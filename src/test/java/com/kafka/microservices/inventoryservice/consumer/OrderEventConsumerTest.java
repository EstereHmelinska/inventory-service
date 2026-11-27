package com.kafka.microservices.inventoryservice.consumer;

import com.kafka.microservices.inventoryservice.dto.OrderCreatedEvent;
import com.kafka.microservices.inventoryservice.service.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

@ExtendWith({OutputCaptureExtension.class, MockitoExtension.class})
class OrderEventConsumerTest {
    @Mock
    private InventoryService inventoryService;

    private static final Long phoneCustomerId = 17L;
    private static final String phoneProduct = "Phone";
    private static final int phoneQuantity = 1;

    private static final Long headphonesCustomerId = 18L;
    private static final String headphonesProduct = "Headphones";
    private static final int headphonesQuantity = 2;

    private OrderEventConsumer orderEventConsumer;

    @BeforeEach
    void setUp() {
        orderEventConsumer = new OrderEventConsumer(inventoryService);
    }

    @Test
    void consumeOrderCreatedEvent_shouldLogOrderDetails(CapturedOutput output) {
        //Arrange
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                phoneCustomerId,
                phoneProduct,
                phoneQuantity,
                LocalDateTime.now()
        );

        //Act
        orderEventConsumer.consumeOrderCreatedEvent(event);

        //Verify
        verify(inventoryService).checkInventory(event);
        //Assert
        assertTrue(output.getOut().contains("Processing order:"));
        assertTrue(output.getOut().contains(event.orderId().toString()));
        assertTrue(output.getOut().contains("Product: " + phoneProduct));
        assertTrue(output.getOut().contains("Quantity: " + phoneQuantity));
    }

    @Test
    //confirms if the consumer isn't accidentally logging fixed values
    void consumeOrderCreatedEvent_shouldHandleDifferentOrderDetails(CapturedOutput output) {
        //Arrange
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                headphonesCustomerId,
                headphonesProduct,
                headphonesQuantity,
                LocalDateTime.now()
        );

        //Act
        orderEventConsumer.consumeOrderCreatedEvent(event);

        //Assert
        assertTrue(output.getOut().contains("Processing order:"));
        assertTrue(output.getOut().contains(event.orderId().toString()));
        assertTrue(output.getOut().contains("Product: " + headphonesProduct));
        assertTrue(output.getOut().contains("Quantity: " + headphonesQuantity));
    }
}
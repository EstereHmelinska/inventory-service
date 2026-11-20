package com.kafka.microservices.inventoryservice.consumer;

import com.kafka.microservices.inventoryservice.dto.OrderCreatedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(OutputCaptureExtension.class)
class OrderEventConsumerTest {

    private OrderEventConsumer orderEventConsumer;

    @BeforeEach
    void setUp() {
        orderEventConsumer = new OrderEventConsumer();
    }

    @Test
    void shouldLogOrderDetails(CapturedOutput output) {
        //Arrange
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                17L,
                "Phone",
                1,
                LocalDateTime.now()
        );

        //Act
        orderEventConsumer.consumeOrderCreatedEvent(event);

        //Assert
        assertTrue(output.getOut().contains("Processing order:"));
        assertTrue(output.getOut().contains(event.orderId().toString()));
        assertTrue(output.getOut().contains("Product: Phone"));
        assertTrue(output.getOut().contains("Quantity: 1"));
    }

    @Test
    //confirms if the consumer isn't accidentally logging fixed values
    void shouldHandleDifferentOrderDetails(CapturedOutput output) {
        //Arrange
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                18L,
                "Headphones",
                2,
                LocalDateTime.now()
        );

        //Act
        orderEventConsumer.consumeOrderCreatedEvent(event);

        //Assert
        assertTrue(output.getOut().contains("Processing order:"));
        assertTrue(output.getOut().contains(event.orderId().toString()));
        assertTrue(output.getOut().contains("Product: Headphones"));
        assertTrue(output.getOut().contains("Quantity: 2"));
    }
}
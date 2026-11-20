package com.kafka.microservices.inventoryservice.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        Long customerId,
        String product,
        int quantity,
        LocalDateTime createdTime) {
}

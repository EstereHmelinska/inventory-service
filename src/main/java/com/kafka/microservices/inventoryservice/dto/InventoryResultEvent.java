package com.kafka.microservices.inventoryservice.dto;

import com.kafka.microservices.inventoryservice.entity.InventoryStatus;

import java.util.UUID;

public record InventoryResultEvent(
        UUID orderId,
        InventoryStatus status) {}
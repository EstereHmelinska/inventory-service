package com.kafka.microservices.inventoryservice.service;

import com.kafka.microservices.inventoryservice.dto.InventoryResultEvent;
import com.kafka.microservices.inventoryservice.dto.OrderCreatedEvent;
import com.kafka.microservices.inventoryservice.entity.InventoryStatus;
import com.kafka.microservices.inventoryservice.producer.InventoryEventProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InventoryService {

    private final InventoryEventProducer inventoryEventProducer;
    private final int availableStock;

    public InventoryService(InventoryEventProducer inventoryEventProducer,
            @Value("${inventory.stock.default-quantity:5}") int availableStock) {
        this.inventoryEventProducer = inventoryEventProducer;
        this.availableStock = availableStock;
    }

    public void checkInventory(OrderCreatedEvent orderEvent) {
        if (orderEvent.quantity() <= 0) {
            throw new IllegalArgumentException(
                    "Order quantity must be greater than zero");
        }

        InventoryStatus status;

        if (orderEvent.quantity() <= availableStock) {
            status = InventoryStatus.AVAILABLE;
        } else {
            status = InventoryStatus.OUT_OF_STOCK;
        }

        InventoryResultEvent resultEvent = new InventoryResultEvent(
                orderEvent.orderId(),
                status
        );

        inventoryEventProducer.publishInventoryResult(resultEvent);
    }
}
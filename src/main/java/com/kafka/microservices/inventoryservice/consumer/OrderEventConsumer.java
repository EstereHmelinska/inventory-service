package com.kafka.microservices.inventoryservice.consumer;

import com.kafka.microservices.inventoryservice.dto.OrderCreatedEvent;
import com.kafka.microservices.inventoryservice.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final InventoryService inventoryService;

    public OrderEventConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "order-created", groupId = "inventory-service-group")
    public void consumeOrderCreatedEvent(OrderCreatedEvent orderCreatedEvent){
        log.info("""
                 Processing order: 
                 OrderId: {}
                 Product: {}
                 Quantity: {}""",
                orderCreatedEvent.orderId(),
                orderCreatedEvent.product(),
                orderCreatedEvent.quantity());
        inventoryService.checkInventory(orderCreatedEvent);
    }

}

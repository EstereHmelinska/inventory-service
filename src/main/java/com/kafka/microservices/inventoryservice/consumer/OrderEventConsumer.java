package com.kafka.microservices.inventoryservice.consumer;

import com.kafka.microservices.inventoryservice.dto.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

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
    }

}

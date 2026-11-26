package com.kafka.microservices.inventoryservice.producer;

import com.kafka.microservices.inventoryservice.dto.InventoryResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);
    private static final String TOPIC = "inventory-result";
    private final KafkaTemplate<String, InventoryResultEvent> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, InventoryResultEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryResult(InventoryResultEvent event) {
        kafkaTemplate.send(
                TOPIC,
                event.orderId().toString(),
                event).whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish inventory result for order {}", event.orderId(), exception);
                    } else {
                        log.info("Published inventory result for order {}", event.orderId());
                    }
        });
    }
}
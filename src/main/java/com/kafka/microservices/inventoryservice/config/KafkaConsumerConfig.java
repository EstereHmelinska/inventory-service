package com.kafka.microservices.inventoryservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configures error handling for Kafka consumers.
 * Retries retryable message processing failures twice with a one-second delay.
 * Logs and skips messages that cannot be deserialized.
 */

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Skipping message from topic {} partition {} offset {} due to processing error",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        exception), new FixedBackOff(1000L, 2L)
        );
    }
}
package com.example.reliableorders.kafka;

import com.example.reliableorders.config.KafkaConfig;
import com.example.reliableorders.model.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<?> send(OrderEvent event) {
        return kafkaTemplate.send(KafkaConfig.ORDERS_TOPIC, event.getOrderId(), event);
    }

    public OrderEvent withGeneratedEventId(OrderEvent event) {
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId(UUID.randomUUID().toString());
        }
        return event;
    }
}

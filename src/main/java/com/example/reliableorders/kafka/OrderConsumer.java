package com.example.reliableorders.kafka;

import com.example.reliableorders.config.KafkaConfig;
import com.example.reliableorders.model.OrderEvent;
import com.example.reliableorders.service.OrderProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class OrderConsumer {

    private final OrderProcessingService processingService;
    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    public OrderConsumer(OrderProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = KafkaConfig.ORDERS_TOPIC, groupId = "order-group")
    public void consume(OrderEvent event) {
        processingService.process(event);
    }

    @KafkaListener(topics = KafkaConfig.DLQ_TOPIC, groupId = "dlq-monitor-group")
    public void monitorDlq(OrderEvent event,
                           @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition) {
        log.error("DLQ message received: eventId={}, orderId={}, partition={}, customerName={}",
                event.getEventId(), event.getOrderId(), partition, event.getCustomerName());
    }
}

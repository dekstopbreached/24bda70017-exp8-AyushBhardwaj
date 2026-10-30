package com.example.reliableorders.web;

import com.example.reliableorders.kafka.OrderProducer;
import com.example.reliableorders.model.OrderEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<OrderEvent> create(@RequestBody OrderEvent event) {
        event.setStatus(event.getStatus() == null ? "CREATED" : event.getStatus());
        producer.withGeneratedEventId(event);
        producer.send(event);
        return ResponseEntity.accepted().body(event);
    }

    @PostMapping("/fail")
    public ResponseEntity<OrderEvent> fail(@RequestBody OrderEvent event) {
        event.setCustomerName("FAIL");
        event.setStatus("FAIL_TEST");
        producer.withGeneratedEventId(event);
        producer.send(event);
        return ResponseEntity.accepted().body(event);
    }

    @PostMapping("/duplicate")
    public ResponseEntity<OrderEvent> duplicate(@RequestBody OrderEvent event) {
        producer.withGeneratedEventId(event);
        event.setStatus(event.getStatus() == null ? "CREATED" : event.getStatus());
        producer.send(event);
        producer.send(event);
        return ResponseEntity.accepted().body(event);
    }
}

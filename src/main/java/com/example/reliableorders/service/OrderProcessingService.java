package com.example.reliableorders.service;

import com.example.reliableorders.exception.RetryableProcessingException;
import com.example.reliableorders.model.OrderEvent;
import com.example.reliableorders.model.ProcessedEvent;
import com.example.reliableorders.repository.ProcessedEventRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

@Service
public class OrderProcessingService {

    private final ProcessedEventRepository processedEventRepository;
    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    public OrderProcessingService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void process(OrderEvent event) {
        if (processedEventRepository.existsById(event.getEventId())) {
            log.info("Duplicate event ignored: eventId={}", event.getEventId());
            return;
        }

        if ("FAIL".equalsIgnoreCase(event.getCustomerName())
                || (event.getAmount() != null && event.getAmount().signum() < 0)) {
            throw new RetryableProcessingException("Simulated processing failure for eventId=" + event.getEventId());
        }

        log.info("Processing order: eventId={}, orderId={}, amount={}",
                event.getEventId(), event.getOrderId(), event.getAmount());

        try {
            processedEventRepository.saveAndFlush(new ProcessedEvent(event.getEventId(), Instant.now()));
        } catch (DataIntegrityViolationException exception) {
            log.info("Duplicate event ignored after concurrent insert: eventId={}", event.getEventId());
        }
    }
}

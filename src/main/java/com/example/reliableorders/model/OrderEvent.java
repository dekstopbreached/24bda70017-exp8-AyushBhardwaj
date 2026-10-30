package com.example.reliableorders.model;

import java.math.BigDecimal;

public class OrderEvent {
    private String eventId;
    private String orderId;
    private String customerName;
    private BigDecimal amount;
    private String status;

    public OrderEvent() {
    }

    public OrderEvent(String eventId, String orderId, String customerName, BigDecimal amount, String status) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.customerName = customerName;
        this.amount = amount;
        this.status = status;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}

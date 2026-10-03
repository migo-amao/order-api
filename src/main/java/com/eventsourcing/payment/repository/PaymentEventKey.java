package com.eventsourcing.payment.repository;

import lombok.Data;

import java.io.Serializable;

@Data
public class PaymentEventKey implements Serializable {
    private String orderId;
    private PaymentEventType eventType;
}

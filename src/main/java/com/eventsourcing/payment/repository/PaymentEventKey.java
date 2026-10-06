package com.eventsourcing.payment.repository;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @AllArgsConstructor
public class PaymentEventKey implements Serializable {
    private String orderId;
    private PaymentEventType eventType;
}

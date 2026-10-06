package com.eventsourcing.payment.repository;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "payment_events")
@IdClass(PaymentEventKey.class)
@Data
public class PaymentEvent {
    @Id @Column(name = "order_id")
    private String orderId;

    @Id @Column(name = "event_type")
    private PaymentEventType eventType;

    private double amount;
    private String notes;

    @Column(name = "event_at")
    private Long eventAt;
}

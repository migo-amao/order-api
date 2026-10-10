package com.eventsourcing.payment.repository;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "payments")
@IdClass(PaymentKey.class)
@Data
public class Payment {
    @Id @Column(name = "order_id")
    private String orderId;

    @Id @Column(name = "payment_type")
    private PaymentType paymentType;

    private double amount;
    private String notes;

    private Long at;
}

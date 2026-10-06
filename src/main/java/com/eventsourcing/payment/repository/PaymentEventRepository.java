package com.eventsourcing.payment.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PaymentEventRepository extends JpaRepository<PaymentEvent, PaymentEventKey> {
    List<PaymentEvent> findTopByOrderIdOrderByEventAtDesc(String orderId);
}
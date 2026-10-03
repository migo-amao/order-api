package com.eventsourcing.payment.repository;

import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentEventRepository extends JpaRepository<PaymentEvent, PaymentEventKey> {}
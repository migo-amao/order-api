package com.eventsourcing.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface OrderEventRepository extends JpaRepository<OrderEvent, OrderEventKey> {
    Optional<OrderEvent> findTopByOrderIdOrderByEventAtDesc(String orderId);
}
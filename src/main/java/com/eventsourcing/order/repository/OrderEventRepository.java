package com.eventsourcing.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface OrderEventRepository extends JpaRepository<OrderEvent, OrderEventKey> {
    List<OrderEvent> findTopByOrderIdOrderByEventAtDesc(String orderId);
}
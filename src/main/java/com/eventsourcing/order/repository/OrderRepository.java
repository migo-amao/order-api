package com.eventsourcing.order.repository;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.domain.OrderKey;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, OrderKey> {
    List<Order> findTopByOrderIdOrderByAtDesc(String orderId);

    List<Order> findAllByUserId(String userId);
}
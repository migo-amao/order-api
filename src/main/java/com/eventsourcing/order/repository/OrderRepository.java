package com.eventsourcing.order.repository;

import com.eventsourcing.order.domain.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {}
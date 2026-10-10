package com.eventsourcing.order.repository;

import com.eventsourcing.order.domain.OrderEvent;
import com.eventsourcing.order.domain.OrderKey;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderEventRepository extends JpaRepository<OrderEvent, OrderKey> {
    List<OrderEvent> findAllByPublished(Boolean published);
}

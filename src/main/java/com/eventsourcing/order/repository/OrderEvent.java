package com.eventsourcing.order.repository;

import com.eventsourcing.order.domain.Order;
import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "order_events")
@IdClass(OrderEventKey.class)
@Data
public class OrderEvent {
    @Id @Column(name = "order_id")
    private String orderId;

    @Id @Column(name = "event_type")
    private OrderEventType eventType;

    @Column(name = "event_at")
    private Long eventAt;
}

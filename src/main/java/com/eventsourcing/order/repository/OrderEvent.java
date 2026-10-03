package com.eventsourcing.order.repository;

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
    @Column(name = "order_details")
    private String orderDetails;
    @Column(name = "event_at")
    private Long eventAt;
}

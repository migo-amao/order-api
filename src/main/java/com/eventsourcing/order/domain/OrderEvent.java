package com.eventsourcing.order.domain;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "order_events")
@IdClass(OrderKey.class)
@Data
public class OrderEvent {
    @Id @Column(name = "order_id")
    private String orderId;

    @Id
    private OrderStatus status;
    
    private Boolean published = false;
}

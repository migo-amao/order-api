package com.eventsourcing.order.repository;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data @AllArgsConstructor
public class OrderEventKey implements Serializable {
    private String orderId;
    private OrderEventType eventType;
}

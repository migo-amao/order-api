package com.eventsourcing.order.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor
public class OrderKey implements Serializable {
    private String orderId;
    private OrderStatus status;
}

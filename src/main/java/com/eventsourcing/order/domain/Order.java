package com.eventsourcing.order.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@Entity
@Table(name = "orders")
@IdClass(OrderKey.class)
public class Order {
    @Id
    @Column(name = "order_id")
    private String orderId;

    @Id
    private OrderStatus status;

    @Column(name = "user_id")
    private String userId;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "order_product_lines", joinColumns = {
            @JoinColumn(name = "order_id", referencedColumnName = "order_id"),
            @JoinColumn(name = "order_status", referencedColumnName = "status"),
    })
    private List<ProductLine> productLines = new ArrayList<>();

    private Long at;
}

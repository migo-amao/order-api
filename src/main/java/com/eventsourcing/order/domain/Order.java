package com.eventsourcing.order.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collection;

@Data @AllArgsConstructor
public class Order {
    private String id;
    private String userId;
    private Collection<ProductLine> productLines;
    private Long createdAt;
}

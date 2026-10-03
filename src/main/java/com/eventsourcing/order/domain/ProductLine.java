package com.eventsourcing.order.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data @AllArgsConstructor
public class ProductLine {
    private String productId;
    private int amount;
    private double unitPrice;
}

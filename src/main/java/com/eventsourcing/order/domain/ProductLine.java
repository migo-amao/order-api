package com.eventsourcing.order.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @AllArgsConstructor @NoArgsConstructor
public class ProductLine {
    private String productId;
    private int amount;
    private double unitPrice;
}

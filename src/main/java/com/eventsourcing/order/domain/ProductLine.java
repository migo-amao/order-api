package com.eventsourcing.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Data;

@Data
@Embeddable
public class ProductLine {
    @Column(name = "product_id")
    private String productId;

    private int amount;

    @Column(name = "unit_price")
    private double unitPrice;
}

package com.eventsourcing.order.domain;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Embeddable
public class ProductLine {
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "product_id")
    private Product product;

    private int amount;

    @Column(name = "unit_price")
    private double unitPrice;
}

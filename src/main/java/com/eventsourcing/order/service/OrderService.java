package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.Order;
import jakarta.validation.constraints.NotNull;

public interface OrderService {
    Order getOrder(@NotNull String orderId);
    String createOrder(@NotNull Order order);
    void cancelOrder(@NotNull String orderId);
}

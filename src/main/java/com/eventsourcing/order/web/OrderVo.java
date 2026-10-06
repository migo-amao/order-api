package com.eventsourcing.order.web;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderEventType;
import com.eventsourcing.payment.repository.PaymentEvent;
import com.eventsourcing.payment.repository.PaymentEventType;
import lombok.Getter;

import java.time.Instant;
import java.time.ZoneId;

@Getter
public class OrderVo {
    private final Order order;
    private OrderStatus orderStatus;
    private PaymentStatus paymentStatus;

    public OrderVo(Order order, OrderEvent orderEvent, PaymentEvent paymentEvent) {
        if(orderEvent != null) {
            this.orderStatus = new OrderStatus(orderEvent.getEventType(), Instant.ofEpochMilli(orderEvent.getEventAt()).atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
        }

        if(paymentEvent != null) {
            this.paymentStatus = new PaymentStatus(paymentEvent.getEventType(), Instant.ofEpochMilli(paymentEvent.getEventAt()).atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
        }

        this.order = order;
    }
}

record OrderStatus(OrderEventType status, String localTime) {}

record PaymentStatus(PaymentEventType status, String localTime) {}

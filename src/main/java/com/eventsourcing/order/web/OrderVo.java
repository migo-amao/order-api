package com.eventsourcing.order.web;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.payment.repository.Payment;
import com.eventsourcing.payment.repository.PaymentType;
import lombok.Getter;

import java.time.Instant;
import java.time.ZoneId;

@Getter
public class OrderVo {
    private final Order order;
    private OrderStatus orderStatus;
    private PaymentStatus paymentStatus;

    public OrderVo(Order order, Payment payment) {
        if(order != null) {
            this.orderStatus = new OrderStatus(order.getStatus(), Instant.ofEpochMilli(order.getAt()).atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
        }

        if(payment != null) {
            this.paymentStatus = new PaymentStatus(payment.getPaymentType(), Instant.ofEpochMilli(payment.getAt()).atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
        }

        this.order = order;
    }
}

record OrderStatus(com.eventsourcing.order.domain.OrderStatus status, String localTime) {}

record PaymentStatus(PaymentType status, String localTime) {}

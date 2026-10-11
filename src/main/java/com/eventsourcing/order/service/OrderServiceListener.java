package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.domain.OrderKey;
import com.eventsourcing.order.domain.OrderStatus;
import com.eventsourcing.order.repository.OrderRepository;
import com.eventsourcing.payment.repository.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

@Service
@Slf4j
public class OrderServiceListener {
    private final OrderService orderService;
    private final OrderRepository orderRepository;

    public OrderServiceListener(OrderService orderService, OrderRepository orderRepository) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "payment-rejected-topic", groupId = "order-consumer-group")
    @RetryableTopic(
            attempts = "2", // Total attempts (1st try + 1 retries)
            dltStrategy = DltStrategy.FAIL_ON_ERROR,    // Send to DLT if all retries fail
            include = {OrderServiceListener.TransientDatabaseException.class}        // ONLY retry on specific transient errors
    )
    public void paymentRejectedEvent(Payment payment) {
        log.debug("Received payment rejected event: {}", payment);

        // Idempotence check if the order was canceled already
        Optional<Order> orderOptional = orderRepository.findById(new OrderKey(payment.getOrderId(), OrderStatus.ORDER_CANCELLED));
        if (orderOptional.isPresent()) {
            Order order = orderOptional.get();
            log.debug("Order {} was already canceled at {}. Ignore it.", order.getOrderId(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(order.getAt()), ZoneId.systemDefault()));
        } else {
            orderService.cancelOrder(payment.getOrderId());
        }

//        throw new TransientDatabaseException("DB error", null);
    }

    private static class TransientDatabaseException extends RuntimeException {
        private TransientDatabaseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    @DltHandler
    public void handleDeadLetter(Payment payment) {
        log.error("Message permanently failed. Logged to DLT: {}", payment);

        // TODO Save to S3?
    }
}
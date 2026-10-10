package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.domain.OrderKey;
import com.eventsourcing.order.domain.OrderStatus;
import com.eventsourcing.order.repository.OrderRepository;
import com.eventsourcing.payment.repository.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
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

    // Because we configured the producer's tx id prefix, a kafka tx manager was auto-injected into listener container
    // Kafka Container starts the transaction using 'kafkaTransactionManager'
    @KafkaListener(topics = "payment-rejected-topic", groupId = "order-consumer-group")
    public void orderCreatedEvent(Payment payment) {
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
    }
}
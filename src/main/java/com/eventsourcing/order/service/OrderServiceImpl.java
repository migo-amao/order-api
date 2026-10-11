package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.*;
import com.eventsourcing.order.repository.OrderEventRepository;
import com.eventsourcing.order.repository.OrderRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class OrderServiceImpl implements OrderService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderRepository orderRepository;
    private final OrderEventRepository orderEventRepository;

    public OrderServiceImpl(KafkaTemplate<String, Object> kafkaTemplate,
                            OrderRepository orderRepository, OrderEventRepository orderEventRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
        this.orderEventRepository = orderEventRepository;
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public String createOrder(Order order) {
        final boolean simulateError = "err".equals(order.getOrderId());
        if (order.getOrderId() == null) {
            String orderId = UUID.randomUUID().toString();
            order.setOrderId(orderId);
        }

        // Idempotence check
        Optional<Order> existingOrder = orderRepository.findById(new OrderKey(order.getOrderId(), OrderStatus.ORDER_CREATED));
        if (existingOrder.isPresent()) {
            log.debug("Order {} was already created. ignore the duplication.", order.getOrderId());
        } else {
            order.setStatus(OrderStatus.ORDER_CREATED);
            order.setAt(Instant.now().toEpochMilli());
            orderRepository.save(order);
        }

        kafkaTemplate.send("order-created-topic", order.getOrderId(), order);

        if (simulateError) {
            throw new RuntimeException("Order creation failed.");
        }

        log.debug("Order created: {}", order);

        return order.getOrderId();
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public void cancelOrder(String orderId) {
        // Idempotence check
        Optional<Order> existingOrder = orderRepository.findById(new OrderKey(orderId, OrderStatus.ORDER_CANCELLED));
        if (existingOrder.isPresent()) {
            log.debug("Order {} was already cancelled. ignore the duplication.", orderId);
        } else {
            Order order = orderRepository.findById(new OrderKey(orderId, OrderStatus.ORDER_CREATED)).orElseThrow(() -> new EntityNotFoundException("Order not found"));
            orderRepository.save(creatCanceledOrder(order));

            // Outbox pattern, save the event in the DB and use a scheduler to publish it.
            OrderEvent event = new OrderEvent();
            event.setOrderId(orderId);
            event.setStatus(OrderStatus.ORDER_CANCELLED);
            orderEventRepository.save(event);

            log.debug("Order cancelled: {}", orderId);
        }
    }

    private Order creatCanceledOrder(Order order) {
        Order canceledOrder = new Order();
        canceledOrder.setOrderId(order.getOrderId());
        canceledOrder.setStatus(OrderStatus.ORDER_CANCELLED);
        canceledOrder.setAt(Instant.now().toEpochMilli());
        canceledOrder.setUserId(order.getUserId());
        for (ProductLine productLine : order.getProductLines()) {
            ProductLine pl = new ProductLine();
            pl.setAmount(productLine.getAmount());
            pl.setProductId(productLine.getProductId());
            pl.setUnitPrice(productLine.getUnitPrice());
            canceledOrder.getProductLines().add(pl);
        }
        return canceledOrder;
    }
}

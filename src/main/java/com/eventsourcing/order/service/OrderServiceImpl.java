package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.domain.ProductLine;
import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderEventKey;
import com.eventsourcing.order.repository.OrderEventRepository;
import com.eventsourcing.order.repository.OrderEventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class OrderServiceImpl implements OrderService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderEventRepository orderEventRepository;

    public OrderServiceImpl(KafkaTemplate<String, Object> kafkaTemplate, OrderEventRepository orderEventRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderEventRepository = orderEventRepository;
    }

    @Override
    public Order getOrder(String orderId) {
        Optional<OrderEvent> orderEvent = orderEventRepository.findTopByOrderIdOrderByEventAtDesc(orderId);
        if (orderEvent.isPresent()) {
            return JsonMapper.builder().build().readerFor(Order.class).readValue(orderEvent.get().getOrderDetails());
        }

        return new Order("order-123",
                "user-123",
                Collections.singletonList(new ProductLine("product-123", 2, 15.5)),
                LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    @Override
    public String createOrder(Order order) {
        if (order.getId() == null) {
            String orderId = UUID.randomUUID().toString();
            order.setId(orderId);
        }
        order.setCreatedAt(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setOrderId(order.getId());
        orderEvent.setEventType(OrderEventType.ORDER_CREATED);
        orderEvent.setOrderDetails(JsonMapper.builder().build().writerFor(Order.class).writeValueAsString(order));
        orderEvent.setEventAt(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        kafkaTemplate.send("order-created-topic", order.getId(), order);

        log.debug("Order created: {}", order);

        return order.getId();
    }

    @Override
    public void cancelOrder(String orderId) {
        // TODO idempotent check if order event db has the order cancelled already

        OrderEvent orderEvent = orderEventRepository.findById(new OrderEventKey(orderId, OrderEventType.ORDER_CREATED)).orElseThrow();
        orderEvent.setEventType(OrderEventType.ORDER_CANCELLED);
        orderEvent.setEventAt(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        kafkaTemplate.send("order-cancelled-topic", orderId, orderEvent);

        log.debug("Order cancelled: {}", orderId);
    }
}

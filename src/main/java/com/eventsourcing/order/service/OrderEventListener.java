package com.eventsourcing.order.service;

import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderEventListener {
    private final OrderEventRepository orderEventRepository;

    public OrderEventListener(OrderEventRepository orderEventRepository) {
        this.orderEventRepository = orderEventRepository;
    }

    @KafkaListener(topics = "order-created-topic", groupId = "order-consumer-group")
    public void orderCreatedEvent(OrderEvent orderEvent) {
        log.debug("Received order created event: {}", orderEvent);
        saveEvent(orderEvent);
    }

    @KafkaListener(topics = "order-cancelled-topic", groupId = "order-consumer-group")
    public void orderCancelledEvent(OrderEvent orderEvent) {
        log.debug("Received order cancelled event: {}", orderEvent);
        saveEvent(orderEvent);
    }

    private void saveEvent(OrderEvent orderEvent) {
        try {
            // TODO idempotent check if order event db has the event saved already
            orderEventRepository.save(orderEvent);
        } catch (Exception ex) {
            log.error("System error when saving order event: {}", orderEvent, ex);

            // TODO retry if it is just network hiccup
        }
    }
}

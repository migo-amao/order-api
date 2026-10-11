package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.OrderEvent;
import com.eventsourcing.order.domain.OrderStatus;
import com.eventsourcing.order.repository.OrderEventRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class OrderEventPublisher {
    private final OrderEventRepository orderEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(OrderEventRepository orderEventRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.orderEventRepository = orderEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

//    @Scheduled(fixedRate = 1000)
    @Transactional(transactionManager = "transactionManager")
    public void publish() {
        List<OrderEvent> orderEvents = orderEventRepository.findAllByPublished(false);
        for (OrderEvent orderEvent : orderEvents) {
            OrderStatus orderStatus = orderEvent.getStatus();
            if (orderStatus == OrderStatus.ORDER_CANCELLED) {
                kafkaTemplate.send("order-canceled-topic", orderEvent.getOrderId(), orderEvent);
            } else if (orderStatus == OrderStatus.ORDER_CREATED){
                kafkaTemplate.send("order-created-topic", orderEvent.getOrderId(), orderEvent);
            }
            orderEvent.setPublished(true);
            orderEventRepository.save(orderEvent);
        }
    }
}

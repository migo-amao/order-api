package com.eventsourcing.order.service;

import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class OrderEventListener {
    private final OrderEventRepository orderEventRepository;

    public OrderEventListener(OrderEventRepository orderEventRepository) {
        this.orderEventRepository = orderEventRepository;
    }

    @KafkaListener(topics = "order-created-topic", groupId = "order-consumer-group")
    // Because we configured the producer's tx id prefix, a kafka tx manager was auto-injected into listener container
    @Transactional(transactionManager = "transactionManager")
    // Db tx was enabled wrapped inside the kafka listener container's tx
    public void orderCreatedEvent(OrderEvent orderEvent) {
        log.debug("Received order created event: {}", orderEvent);

        // Will be committed after method exits. Thoughts: because db tx will be committed first followed by the consumer's offset via the
        // listener's tx, there is an edge case that db tx succeeded but listener container's tx failed. The message will be re-read again
        // next time. It is important to make the db operation as idempotent.
        saveEvent(orderEvent);
    }

    @KafkaListener(topics = "order-cancelled-topic", groupId = "order-consumer-group")
    @Transactional(transactionManager = "transactionManager")
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

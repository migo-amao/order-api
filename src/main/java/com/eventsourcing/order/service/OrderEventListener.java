package com.eventsourcing.order.service;

import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderEventKey;
import com.eventsourcing.order.repository.OrderEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@Slf4j
public class OrderEventListener {
    private final OrderEventRepository orderEventRepository;

    public OrderEventListener(OrderEventRepository orderEventRepository) {
        this.orderEventRepository = orderEventRepository;
    }

    // Because we configured the producer's tx id prefix, a kafka tx manager was auto-injected into listener container
    // Kafka Container starts the transaction using 'kafkaTransactionManager'
    @KafkaListener(topics = "order-created-topic", groupId = "order-consumer-group")
    // Db tx was enabled wrapped inside the kafka listener container's tx
    @Transactional(transactionManager = "transactionManager")
    /*
    Non-Blocking Redelivery: Instead of locking up your main consumer partition, Spring Kafka forwards the original message to a separate,
    automatically generated retry topic (e.g., order-event-retry-2000) and commits the offset on the main topic.
    By default, @RetryableTopic uses Non-Blocking Retry (forwarding messages to a retry topic). This is excellent for performance because
    a single broken message won't block other healthy messages behind it.
    However, because messages are moved to a retry topic, message ordering is lost. If absolute strict message ordering per partition is required
    for your business logic, you must switch to Blocking Retry via your YAML configuration or a local DefaultErrorHandler bean instead of using
    this annotation.
     */
    @RetryableTopic(
            attempts = "3", // Total attempts (1st try + 1 retries)
            backOff = @BackOff(delay = 2000, multiplier = 2), // 2s, then 4s, then 8s
            dltStrategy = DltStrategy.FAIL_ON_ERROR,    // Send to DLT if all retries fail
            include = {TransientDatabaseException.class}        // ONLY retry on specific transient errors
    )
    public void orderCreatedEvent(OrderEvent orderEvent) {
        log.debug("Received order created event: {}", orderEvent);

        // Will be committed after method exits. Thoughts: because db tx will be committed first followed by the consumer's offset via the
        // listener's tx, there is an edge case that db tx succeeded but listener container's tx failed. The message will be re-read again
        // next time. It is important to make the db operation as idempotent.
        saveEvent(orderEvent);
        if (orderEvent.getOrderId().hashCode() % 5 == 0) {
            throw new TransientDatabaseException("Simulated db error", null);
        }
        // End of method: DB commits first. If successful, Kafka commits its offsets and messages to the same tx log.
    }

    @KafkaListener(topics = "order-cancelled-topic", groupId = "order-consumer-group")
    @Transactional(transactionManager = "transactionManager")
    public void orderCancelledEvent(OrderEvent orderEvent) {
        log.debug("Received order cancelled event: {}", orderEvent);
        saveEvent(orderEvent);
    }

    private void saveEvent(OrderEvent orderEvent) {
        // idempotent check if order event db has the event saved already
        Optional<OrderEvent> orderEventOptional = orderEventRepository.findById(new OrderEventKey(orderEvent.getOrderId(), orderEvent.getEventType()));
        if (orderEventOptional.isEmpty()) {
            orderEventRepository.save(orderEvent);
        }
    }

    private static class TransientDatabaseException extends RuntimeException {
        private TransientDatabaseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    // Optional: This method handles messages that failed all 4 attempts
    @DltHandler
    public void handleDeadLetter(OrderEvent orderEvent) {
        log.error("Message permanently failed. Logged to DLT: {}", orderEvent);

        // TODO Save to S3?
    }
}

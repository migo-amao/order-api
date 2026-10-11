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

    @PreAuthorize("hasRole('ADMIN')")
    @Override
    /* Enable Jpa tx manager
       If you are only producing messages to Kafka and updating a database (Producer-only transactions),
       you do not need a KafkaTransactionManager bean at all.
       When you annotate a method with @Transactional pointing to your database, KafkaTemplate automatically detects the active DB transaction.
       It binds a Kafka transaction to it via Spring's TransactionSynchronizationManager. When your method returns,
       the database transaction commits first, followed immediately by the Kafka transaction.

       The kafka template producer needs to attach to a tx. If no @Transactional here, the producer will throw exception
       java.lang.IllegalStateException: No transaction is in process; possible solutions: run the template operation within the scope of a template.executeInTransaction() operation,
       start a transaction with @Transactional before invoking the template method, run in a transaction started by a listener container when consuming a record
     */
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

        /*
        THoughts:
          there is a partial failure edge case. After the method exits, Jpa tx manager will commit the db changes, and then the kafka tx manager
          commits the message sending via the Tx Sync Manager's call back. What if db tx succeeded but kafka tx failed? It ends up the message in kafka
          marked as ABORTED but db has data permanent. 2 alternatives: 1) change to Outbox pattern. The event is saved in a different table in the same db
          tx as the order. Then a scheduled poller pulls the event and publish it. We want the poller has tx db (because we will update the event status as
          processed) and NON-TX kafka template (because we want the message to be acked immediately). This way if db failed, ending with non-processed status
          still. When this event was retried next round (because status is non-processed), we just have a duplicated message. We convert the partial failure to
          duplicated message (worst case). Since kafka listener is always implemented with idempotency check. This is acceptable. We also don't want to have 2
          poller instances, because it will be in a race condition to process the same event 2 times.
          2) don't save order here. just publish it together with event using a non-tx template. make the event publishing sync like send().get().
          so we know this event is live immediately on the broker.
         */

        return order.getOrderId();
    }

    @PreAuthorize("hasRole('ADMIN')")
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

package com.eventsourcing.order.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.domain.Product;
import com.eventsourcing.order.domain.ProductLine;
import com.eventsourcing.order.domain.User;
import com.eventsourcing.order.repository.*;
import com.eventsourcing.payment.repository.PaymentEventRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final UserRepository userRepository;
    private final ProductRepository productRepository;

    public OrderServiceImpl(KafkaTemplate<String, Object> kafkaTemplate,
                            OrderEventRepository orderEventRepository,
                            OrderRepository orderRepository,
                            UserRepository userRepository,
                            ProductRepository productRepository,
                            PaymentEventRepository paymentEventRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
        this.userRepository = userRepository;
        this.productRepository = productRepository;
    }

    @Override
    public Order getOrder(String orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new EntityNotFoundException("Order not found"));
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    /* Enable Jpa tx manager, Tx Sync Manager that syncs kafka tx manager behind the Jpa tx manager.
       The kafka tx manager was enabled by the producer tx prefix configuration */
    public String createOrder(Order order) {
        // Every order creation is treated as a new order. No idempotent check needed.
        if (order.getId() == null) {
            String orderId = UUID.randomUUID().toString();
            order.setId(orderId);
        }

        Optional<User> user = userRepository.findById(order.getUser().getId());
        if (user.isEmpty()) {
            userRepository.save(order.getUser());
        }
        for (ProductLine productLine : order.getProductLines()) {
            Optional<Product> product = productRepository.findById(productLine.getProduct().getId());
            if (product.isEmpty()) {
                productRepository.save(productLine.getProduct());
            }
        }

        // Save the order, will be committed in the db tx after method exits
        orderRepository.save(order);

        // Publish the order created event
        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setOrderId(order.getId());
        orderEvent.setEventType(OrderEventType.ORDER_CREATED);
        orderEvent.setEventAt(Instant.now().toEpochMilli()); // UTC time in milliseconds

        kafkaTemplate.send("order-created-topic", order.getId(), orderEvent);

        log.debug("Order created: {}", order);

        // TODO controller advisor for the exception handling

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

        return order.getId();
    }

    @Override
    /* The kafka template producer needs to attach to a tx. If no @Transactional here, the producer will throw exception
    *  java.lang.IllegalStateException: No transaction is in process; possible solutions: run the template operation within the scope of a template.executeInTransaction() operation,
    *  start a transaction with @Transactional before invoking the template method, run in a transaction started by a listener container when consuming a record
    *  Either @Transactional works*/
//    @Transactional(readOnly = true, transactionManager = "transactionManager")
    @Transactional(transactionManager = "kafkaTransactionManager") // This will keep the db connection open for the lazy loading objects
    public void cancelOrder(String orderId) {
        // No idempotent check is needed, because it doesn't make any modification to order.

        // TODO controller advisor handle it
        Order order = orderRepository.findById(orderId).orElseThrow(() -> new EntityNotFoundException("Order not found"));

        log.debug("Found order to be cancelled: {}", order);

        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setOrderId(orderId);
        orderEvent.setEventType(OrderEventType.ORDER_CANCELLED);
        orderEvent.setEventAt(Instant.now().toEpochMilli());

        kafkaTemplate.send("order-cancelled-topic", orderId, orderEvent);

        log.debug("Order cancelled: {}", orderId);

//        throw new RuntimeException("cancel order failed");
    }
}

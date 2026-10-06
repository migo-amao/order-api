package com.eventsourcing.payment.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderRepository;
import com.eventsourcing.payment.repository.PaymentEvent;
import com.eventsourcing.payment.repository.PaymentEventType;
import jakarta.persistence.EntityNotFoundException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;

@Service
@Slf4j
public class PaymentServiceListener {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderRepository orderRepository;

    public PaymentServiceListener(KafkaTemplate<String, Object> kafkaTemplate, OrderRepository orderRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "order-created-topic", groupId = "payment-consumer-group")
    /* Add it, even though there is no db operation, because this will make sure kafka templates send operation and the consumer's
       offset can be committed together at last. The above statement may not be true. See the order credit method below, where it doesn't
       have @Transactional */
    @Transactional(transactionManager = "transactionManager")
    public void orderCreatedEvent(OrderEvent orderEvent) {
        log.debug("Received order created event: {}", orderEvent);

        // TODO idempotent check - if payment db has this order charged success, skip

        // Charge the order
        Order order = getOrder(orderEvent);
        PaymentEvent paymentEvent = createPaymentEvent(order);
        paymentEvent.setEventType(PaymentEventType.PAYMENT_SUCCESS);
        // Charging order could fail due to system issue like network, or the downstream service is down
        try {
            PaymentDetails paymentDetails = chargeOrder(order);
            if (!paymentDetails.result) {
                paymentEvent.setEventType(PaymentEventType.PAYMENT_ERROR);
                paymentEvent.setNotes(paymentDetails.getNotes());
            }
        } catch (Exception ex) {
            paymentEvent.setEventType(PaymentEventType.PAYMENT_ERROR);
            paymentEvent.setNotes(ex.getMessage());
            log.error("System error when charging order: {}", order.getId(), ex);

            // TODO retry if network hiccup
        }

        if (paymentEvent.getEventType() == PaymentEventType.PAYMENT_ERROR) {
            kafkaTemplate.send("payment-error-topic", order.getId(), paymentEvent);
        } else {
            kafkaTemplate.send("payment-success-topic", order.getId(), paymentEvent);
        }

        log.debug("Published payment event: {}", paymentEvent);
    }

    @KafkaListener(topics = "order-cancelled-topic", groupId = "payment-consumer-group")
    /* No need to add @Transactional if no db write operation, since the kafka listener container have the tx manager injected because of the producer
       transaction id prefix configuration. The kafka sent message will be aborted and consumer offset will not be committed. When the listener back to
       live, the message was consumed again.*/
//    @Transactional(readOnly = true, transactionManager = "transactionManager")
    public void orderCancelledEvent(OrderEvent orderEvent) {
        log.debug("Received order cancelled event: {}", orderEvent);

        // TODO idempotent check - if payment db has this order credit success, skip

        // Credit the order
        Order order = getOrder(orderEvent);
        PaymentEvent paymentEvent = createPaymentEvent(order);
        paymentEvent.setEventType(PaymentEventType.CREDIT_SUCCESS);
        // Crediting order could fail due to system issue like network, or the downstream service is down
        try {
            PaymentDetails paymentDetails = creditOrder(order);
            if (!paymentDetails.result) {
                paymentEvent.setEventType(PaymentEventType.CREDIT_ERROR);
                paymentEvent.setNotes(paymentDetails.getNotes());
            }
        } catch (Exception ex) {
            paymentEvent.setEventType(PaymentEventType.CREDIT_ERROR);
            paymentEvent.setNotes(ex.getMessage());
            log.error("System error when crediting order: {}", order.getId(), ex);

            // TODO retry if network hiccup
        }

        if (paymentEvent.getEventType() == PaymentEventType.CREDIT_ERROR) {
            kafkaTemplate.send("credit-error-topic", order.getId(), paymentEvent);
        } else {
            kafkaTemplate.send("credit-success-topic", order.getId(), paymentEvent);
        }

        log.debug("Published payment credit event: {}", paymentEvent);

//        throw new RuntimeException("Credit order failed");
    }

    private Order getOrder(OrderEvent orderEvent) {
        return orderRepository.findById(orderEvent.getOrderId()).orElseThrow(() -> new EntityNotFoundException("Order not found"));
    }

    private PaymentEvent createPaymentEvent(Order order) {
        PaymentEvent paymentEvent = new PaymentEvent();
        paymentEvent.setAmount(getOrderTotal(order));
        paymentEvent.setOrderId(order.getId());
        paymentEvent.setEventAt(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        return paymentEvent;
    }

    private double getOrderTotal(Order order) {
        return order.getProductLines().stream().map(productLine -> productLine.getUnitPrice() * productLine.getAmount()).reduce(0.0, Double::sum);
    }

    private PaymentDetails chargeOrder(Order order) {
        double total = getOrderTotal(order);

        PaymentDetails details = new PaymentDetails();
        if (total > 100) {
            details.setResult(false);
            details.setNotes("Insufficient funds.");
        } else {
            details.setResult(true);
        }

        return details;
    }

    private PaymentDetails creditOrder(Order order) {
        double total = getOrderTotal(order);

        PaymentDetails details = new PaymentDetails();
        if (total <= 100) {
            details.setResult(true);
        } else {
            details.setResult(false);
            details.setNotes("Insufficient funds.");
        }

        return details;
    }

    @Data
    private static final class PaymentDetails {
        private boolean result;
        private String notes;
    }
}

package com.eventsourcing.payment.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.payment.repository.PaymentEvent;
import com.eventsourcing.payment.repository.PaymentEventType;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.time.ZoneId;

@Service
@Slf4j
public class PaymentServiceListener {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentServiceListener(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "order-created-topic", groupId = "payment-consumer-group")
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
    }

    private Order getOrder(OrderEvent orderEvent) {
        JsonMapper mapper = JsonMapper.builder().build();
        return mapper.readerFor(Order.class).readValue(orderEvent.getOrderDetails());
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

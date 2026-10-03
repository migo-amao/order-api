package com.eventsourcing.payment.service;

import com.eventsourcing.payment.repository.PaymentEvent;
import com.eventsourcing.payment.repository.PaymentEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentEventListener {
    private final PaymentEventRepository paymentEventRepository;

    public PaymentEventListener(PaymentEventRepository paymentEventRepository) {
        this.paymentEventRepository = paymentEventRepository;
    }

    @KafkaListener(topics = "payment-success-topic", groupId = "payment-consumer-group")
    public void paymentSuccessEvent(PaymentEvent paymentEvent) {
        log.debug("Received payment success event for order: {}", paymentEvent.getOrderId());
        saveEvent(paymentEvent);
    }

    @KafkaListener(topics = "payment-error-topic", groupId = "payment-consumer-group")
    public void paymentErrorEvent(PaymentEvent paymentEvent) {
        log.debug("Received payment error event for order: {}, {}", paymentEvent.getOrderId(), paymentEvent.getNotes());
        saveEvent(paymentEvent);
    }

    @KafkaListener(topics = "credit-success-topic", groupId = "payment-consumer-group")
    public void creditSuccessEvent(PaymentEvent paymentEvent) {
        log.debug("Received credit success event for order: {}", paymentEvent.getOrderId());
        saveEvent(paymentEvent);
    }

    @KafkaListener(topics = "credit-error-topic", groupId = "payment-consumer-group")
    public void creditErrorEvent(PaymentEvent paymentEvent) {
        log.debug("Received credit error event for order: {}, {}", paymentEvent.getOrderId(), paymentEvent.getNotes());
        saveEvent(paymentEvent);
    }

    private void saveEvent(PaymentEvent paymentEvent) {
        try {
            // TODO idempotent check if payment event db has the event saved already
            paymentEventRepository.save(paymentEvent);
        } catch (Exception ex) {
            log.error("System error when saving payment event: {}", paymentEvent, ex);

            // TODO retry if it is just network hiccup
        }
    }
}

package com.eventsourcing.payment.service;

import com.eventsourcing.payment.repository.PaymentEvent;
import com.eventsourcing.payment.repository.PaymentEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class PaymentEventListener {
    private final PaymentEventRepository paymentEventRepository;

    public PaymentEventListener(PaymentEventRepository paymentEventRepository) {
        this.paymentEventRepository = paymentEventRepository;
    }

    @KafkaListener(topics = "payment-success-topic", groupId = "payment-consumer-group")
    @Transactional(transactionManager = "transactionManager")
    public void paymentSuccessEvent(PaymentEvent paymentEvent) {
        log.debug("Received payment success event for order: {}", paymentEvent.getOrderId());
        saveEvent(paymentEvent);
        // Notes: method level transaction enabled (@Transactional), which means the db operation will commit after method exits.
        // If something goes wrong here, the consumer offset will be rolled back by the listener container's tx manager.
        // Also, the db tx manager will roll back the db change too. Db will remain unchanged.
        // Therefore, the message will be consumed again and db will be changed when this listener is back to live. This is exactly once processing.
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
        // Notes: no method level transaction enabled (@Transactional), which means the db operation will succeed but if something goes wrong here,
        // the consumer offset will be rolled back by the listener container's tx manager. Therefore, the message will be consumed again when
        // this listener is back to live. But Db has the changes already. Since Db has this change already, it won't try to re-write it to force
        // a constraint violation exception. It behaved like "exactly once processing".
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

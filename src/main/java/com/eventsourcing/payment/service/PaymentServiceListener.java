package com.eventsourcing.payment.service;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.domain.OrderEvent;
import com.eventsourcing.payment.repository.Payment;
import com.eventsourcing.payment.repository.PaymentKey;
import com.eventsourcing.payment.repository.PaymentRepository;
import com.eventsourcing.payment.repository.PaymentType;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

@Service
@Slf4j
public class PaymentServiceListener {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentRepository paymentRepository;

    public PaymentServiceListener(KafkaTemplate<String, Object> kafkaTemplate, PaymentRepository paymentRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentRepository = paymentRepository;
    }

    @KafkaListener(topics = "order-created-topic", groupId = "payment-consumer-group")
    // Notes: method level DB transaction enabled (@Transactional), which means the db operation will commit after method exits.
    // If something goes wrong in the method, the consumer offset will be rolled back by the listener container's tx manager (kafkaTransactionManager).
    // Also, the db tx manager will roll back the db change. Db will remain unchanged. The message will be consumed again when this
    // listener is back to live. This is exactly once processing. There is an edge case, that after method exits, DB tx will be
    // committed first followed by the Kafka tx. If DB succeeded but Kafka failed, the message will be consumed again, causing duplicated message.
    // All listeners should be idempotent capable.
    @Transactional(transactionManager = "transactionManager")
    public void orderCreatedEvent(Order order) {
        log.debug("Received order created event: {}", order);

        Payment payment;

        // Idempotence check to avoid double charge
        Optional<Payment> paymentOptional = paymentRepository.findById(new PaymentKey(order.getOrderId(), PaymentType.PAYMENT_SUCCESS));
        if (paymentOptional.isPresent()) {
            payment = paymentOptional.get();
            log.debug("Order {} was charged already at {}. Ignore it.", payment.getOrderId(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(payment.getAt()), ZoneId.systemDefault()));
        } else {
            // Charge the order
            PaymentDetails paymentDetails = chargeOrder(order);

            payment = createPayment(order);
            payment.setPaymentType(PaymentType.PAYMENT_SUCCESS);
            if (!paymentDetails.result) {
                payment.setPaymentType(PaymentType.PAYMENT_REJECTED);
                payment.setNotes(paymentDetails.getNotes());
            }
            paymentRepository.save(payment);
        }

        if (payment.getPaymentType() == PaymentType.PAYMENT_REJECTED) {
            kafkaTemplate.send("payment-rejected-topic", order.getOrderId(), payment);
        } else {
            kafkaTemplate.send("payment-success-topic", order.getOrderId(), payment);
        }
    }

    @KafkaListener(topics = "order-canceled-topic", groupId = "payment-consumer-group")
    /* Since the kafka listener container have the tx manager injected because of the producer
       transaction id prefix configuration. The kafka sent message will be aborted and consumer offset will not be committed. When the listener back to
       live, the message was consumed again. No method level DB transaction enabled (@Transactional), which means
       the db operation will succeed immediately. But if something goes wrong afterward,
       the consumer offset will be rolled back by the listener container's tx manager. Therefore, the message will be consumed again when
       this listener is back to live. But Db has the changes already, same as order creation listener's edge case. So all listeners need to be idempotent.
     */
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
            attempts = "3", // Total attempts (1st try + 2 retries)
            backOff = @BackOff(delay = 2000, multiplier = 2), // 2s, then 4s, then 8s
            dltStrategy = DltStrategy.FAIL_ON_ERROR,    // Send to DLT if all retries fail
            include = {TransientDatabaseException.class}        // ONLY retry on specific transient errors
    )
    public void orderCanceledEvent(OrderEvent orderEvent) {
        // Remember we used the Outbox pattern for order cancelling?
        log.debug("Received order cancelled event: {}", orderEvent);

        Payment payment;

        // Idempotence check to avoid double credit
        Optional<Payment> paymentOptional = paymentRepository.findById(new PaymentKey(orderEvent.getOrderId(), PaymentType.CREDIT_SUCCESS));
        if (paymentOptional.isPresent()) {
            payment = paymentOptional.get();
            log.debug("Order {} was credited already at {}. Ignore it.", payment.getOrderId(),
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(payment.getAt()), ZoneId.systemDefault()));
        } else {
            paymentOptional = paymentRepository.findById(new PaymentKey(orderEvent.getOrderId(), PaymentType.PAYMENT_SUCCESS));
            if (paymentOptional.isPresent()) {
                payment = paymentOptional.get();

                // Credit the order
                PaymentDetails paymentDetails = creditOrder(payment);

                payment = new Payment();
                payment.setOrderId(orderEvent.getOrderId());
                payment.setPaymentType(PaymentType.CREDIT_SUCCESS);
                payment.setAmount(0 - paymentOptional.get().getAmount());
                payment.setAt(Instant.now().toEpochMilli());
                if (!paymentDetails.result) {
                    payment.setPaymentType(PaymentType.CREDIT_REJECTED);
                    payment.setNotes(paymentDetails.getNotes());
                }
                paymentRepository.save(payment);

                if (payment.getPaymentType() == PaymentType.CREDIT_REJECTED) {
                    kafkaTemplate.send("credit-rejected-topic", payment.getOrderId(), payment);
                } else {
                    kafkaTemplate.send("credit-success-topic", payment.getOrderId(), payment);
                }
            } else {
                log.debug("This order was not charged yet, no need to credit it: {}", orderEvent.getOrderId());
            }
        }
    }

    private Payment createPayment(Order order) {
        Payment payment = new Payment();
        payment.setAmount(getOrderTotal(order));
        payment.setOrderId(order.getOrderId());
        payment.setAt(Instant.now().toEpochMilli());
        return payment;
    }

    private double getOrderTotal(Order order) {
        return order.getProductLines().stream().map(productLine -> productLine.getUnitPrice() * productLine.getAmount()).reduce(0.0, Double::sum);
    }

    private PaymentDetails chargeOrder(Order order) {
        // TODO since this is a 3rd party API, it needs to do idempotence check if the order was already charged

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

    private PaymentDetails creditOrder(Payment payment) {
        // TODO since this is a 3rd party API, it needs to do Idempotence check to avoid double credit

        double total = payment.getAmount();

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

    private static class TransientDatabaseException extends RuntimeException {
        private TransientDatabaseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    @DltHandler
    public void handleDeadLetter(OrderEvent orderEvent) {
        log.error("Message permanently failed. Logged to DLT: {}", orderEvent);

        // TODO Save to S3?
    }
}

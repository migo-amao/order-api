package com.eventsourcing.payment.repository;

public enum PaymentEventType {
    PAYMENT_SUCCESS,
    PAYMENT_ERROR,
    CREDIT_SUCCESS,
    CREDIT_ERROR
}

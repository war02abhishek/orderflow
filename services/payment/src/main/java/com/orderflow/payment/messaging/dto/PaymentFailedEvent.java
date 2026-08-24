package com.orderflow.payment.messaging.dto;

public record PaymentFailedEvent(String orderId, String reason) {
}

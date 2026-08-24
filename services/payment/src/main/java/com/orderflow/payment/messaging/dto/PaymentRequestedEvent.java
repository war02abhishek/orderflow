package com.orderflow.payment.messaging.dto;

public record PaymentRequestedEvent(String orderId, String sku, int quantity) {
}

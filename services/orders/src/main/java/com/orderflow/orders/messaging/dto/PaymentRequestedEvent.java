package com.orderflow.orders.messaging.dto;

public record PaymentRequestedEvent(String orderId, String sku, int quantity) {
}

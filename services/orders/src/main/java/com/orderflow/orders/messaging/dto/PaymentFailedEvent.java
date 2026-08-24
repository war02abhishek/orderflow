package com.orderflow.orders.messaging.dto;

public record PaymentFailedEvent(String orderId, String reason) {
}

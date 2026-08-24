package com.orderflow.notifications.messaging.dto;

public record OrderCancelledEvent(String orderId, String sku, int quantity, String reason) {
}

package com.orderflow.notifications.messaging.dto;

public record OrderPlacedEvent(String orderId, String sku, int quantity) {
}

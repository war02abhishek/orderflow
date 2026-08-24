package com.orderflow.orders.messaging.dto;

public record OrderPlacedEvent(String orderId, String sku, int quantity) {
}

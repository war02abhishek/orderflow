package com.orderflow.orders.client.dto;

public record InventoryReserveResponse(
        String sku,
        int requestedQty,
        int remaining,
        boolean success,
        String errorCode,
        String handledByPod) {
}

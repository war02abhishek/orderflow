package com.orderflow.inventory.web.dto;

public record ReserveResponse(
        String sku,
        int requestedQty,
        int remaining,
        boolean success,
        String errorCode,
        String handledByPod) {
}

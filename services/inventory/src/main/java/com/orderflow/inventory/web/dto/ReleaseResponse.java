package com.orderflow.inventory.web.dto;

public record ReleaseResponse(
        String sku,
        int releasedQty,
        int remaining,
        String handledByPod) {
}

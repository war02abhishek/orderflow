package com.orderflow.orders.web.dto;

import com.orderflow.orders.domain.Order;
import com.orderflow.orders.support.PodInfo;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String sku,
        int quantity,
        String status,
        Instant createdAt,
        String handledByPod) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getSku(),
                order.getQuantity(),
                order.getStatus().name(),
                order.getCreatedAt(),
                PodInfo.hostname());
    }
}

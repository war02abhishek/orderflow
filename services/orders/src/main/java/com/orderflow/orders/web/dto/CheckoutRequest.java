package com.orderflow.orders.web.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CheckoutRequest(@NotBlank String sku, @Min(1) int quantity) {
}

package com.orderflow.inventory.web.dto;

import jakarta.validation.constraints.Min;

public record QuantityRequest(@Min(1) int quantity) {
}

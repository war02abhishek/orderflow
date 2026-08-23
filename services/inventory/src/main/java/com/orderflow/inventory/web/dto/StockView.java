package com.orderflow.inventory.web.dto;

public record StockView(String sku, int available, String handledByPod) {
}

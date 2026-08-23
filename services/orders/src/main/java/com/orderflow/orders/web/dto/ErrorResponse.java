package com.orderflow.orders.web.dto;

public record ErrorResponse(String errorCode, String message, String handledByPod) {
}

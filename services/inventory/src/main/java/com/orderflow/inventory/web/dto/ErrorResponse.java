package com.orderflow.inventory.web.dto;

public record ErrorResponse(String errorCode, String message, String handledByPod) {
}

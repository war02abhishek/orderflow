package com.orderflow.orders.domain;

/**
 * Phase 1 only covers the synchronous stock-reservation precondition — the
 * saga states (AWAITING_PAYMENT, CONFIRMED, CANCELLED) land in Phase 2 once
 * the payment step exists.
 */
public enum OrderStatus {
    RESERVED,
    REJECTED
}

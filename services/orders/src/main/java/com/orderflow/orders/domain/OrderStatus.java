package com.orderflow.orders.domain;

/**
 * REJECTED is the only immediate/synchronous terminal state (stock wasn't
 * available). Everything past a successful reservation goes through the
 * saga: AWAITING_PAYMENT until payment resolves, then CONFIRMED or
 * CANCELLED (the compensated path -- inventory gets released).
 */
public enum OrderStatus {
    AWAITING_PAYMENT,
    CONFIRMED,
    CANCELLED,
    REJECTED
}

package com.orderflow.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The durable backstop half of G17's two-tier idempotency check. Redis
 * answers "have I seen this order before" in under a millisecond for the
 * common case; this table is what survives a Redis restart. The primary
 * key is the order ID itself -- a second INSERT for the same order hits
 * the constraint and is treated as a duplicate, the same pattern G2 uses
 * on the inventory side.
 */
@Entity
@Table(name = "processed_event")
@Getter
@Setter
@NoArgsConstructor
public class ProcessedEvent {

    @Id
    private String orderId;

    @Column(nullable = false)
    private Instant processedAt;

    public ProcessedEvent(String orderId, Instant processedAt) {
        this.orderId = orderId;
        this.processedAt = processedAt;
    }
}

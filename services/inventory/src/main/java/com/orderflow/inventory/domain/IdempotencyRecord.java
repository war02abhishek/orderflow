package com.orderflow.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One row per (Idempotency-Key) seen by /reserve or /release. A retried
 * request — including Istio's own retry policy from Phase 4 — looks itself
 * up here instead of mutating stock a second time (G2).
 */
@Entity
@Table(name = "idempotency_record")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IdempotencyRecord {

    @Id
    private String idempotencyKey;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private String operation;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false)
    private boolean success;

    private String errorCode;

    private Integer remainingAfter;

    @Column(nullable = false)
    private Instant createdAt;
}

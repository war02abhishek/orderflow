package com.orderflow.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Dedup for StockReconciler: the Redis Stream entry ID is the primary key,
 * so a redelivered stream entry (consumer crashed after applying the delta
 * but before XACK) is recognized and skipped instead of double-applying
 * the same delta to Postgres.
 */
@Entity
@Table(name = "reconciled_stream_event")
@Getter
@Setter
@NoArgsConstructor
public class ReconciledEvent {

    @Id
    private String streamEntryId;

    @Column(nullable = false)
    private Instant reconciledAt;

    public ReconciledEvent(String streamEntryId, Instant reconciledAt) {
        this.streamEntryId = streamEntryId;
        this.reconciledAt = reconciledAt;
    }
}

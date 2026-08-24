package com.orderflow.notifications.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * G17's durable backstop, same pattern as payment's ProcessedEvent. The key
 * includes the event type since one order can legitimately produce two
 * different notification events over its lifetime (OrderPlaced OR
 * OrderCancelled, never both, but the key still disambiguates them).
 */
@Entity
@Table(name = "processed_event")
@Getter
@Setter
@NoArgsConstructor
public class ProcessedEvent {

    @Id
    private String dedupeKey;

    @Column(nullable = false)
    private Instant processedAt;

    public ProcessedEvent(String dedupeKey, Instant processedAt) {
        this.dedupeKey = dedupeKey;
        this.processedAt = processedAt;
    }
}

package com.orderflow.inventory.service;

import com.orderflow.inventory.domain.IdempotencyRecord;
import com.orderflow.inventory.domain.Stock;
import com.orderflow.inventory.exception.SkuNotFoundException;
import com.orderflow.inventory.repo.IdempotencyRepository;
import com.orderflow.inventory.repo.StockRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class InventoryService {

    private static final String RESERVE = "RESERVE";
    private static final String RELEASE = "RELEASE";
    private static final String INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK";

    private final StockRepository stockRepository;
    private final IdempotencyRepository idempotencyRepository;

    public record Outcome(boolean success, int remaining, String errorCode) {
    }

    @Transactional
    public Outcome reserve(String sku, int qty, String idempotencyKey) {
        return withIdempotency(idempotencyKey, sku, RESERVE, qty, () -> {
            int updated = stockRepository.reserveAtomic(sku, qty);
            int remaining = currentStock(sku);
            return updated > 0
                    ? new Outcome(true, remaining, null)
                    : new Outcome(false, remaining, INSUFFICIENT_STOCK);
        });
    }

    @Transactional
    public Outcome release(String sku, int qty, String idempotencyKey) {
        return withIdempotency(idempotencyKey, sku, RELEASE, qty, () -> {
            stockRepository.releaseAtomic(sku, qty);
            return new Outcome(true, currentStock(sku), null);
        });
    }

    public int currentStock(String sku) {
        return stockRepository.findById(sku)
                .orElseThrow(() -> new SkuNotFoundException(sku))
                .getAvailableQty();
    }

    /**
     * Looks up the idempotency key first (G2): a retried request — from the
     * caller or from Istio's own retry policy in Phase 4 — replays the
     * stored outcome instead of mutating stock again. A same-key race that
     * slips past the initial lookup still can't double-mutate: the second
     * writer hits the idempotency table's primary-key constraint and falls
     * back to reading the first writer's result.
     */
    private Outcome withIdempotency(String idempotencyKey, String sku, String operation, int qty,
            java.util.function.Supplier<Outcome> action) {
        var existing = idempotencyRepository.findById(idempotencyKey);
        if (existing.isPresent()) {
            return toOutcome(existing.get());
        }

        Outcome outcome = action.get();

        IdempotencyRecord record = new IdempotencyRecord();
        record.setIdempotencyKey(idempotencyKey);
        record.setSku(sku);
        record.setOperation(operation);
        record.setQuantity(qty);
        record.setSuccess(outcome.success());
        record.setErrorCode(outcome.errorCode());
        record.setRemainingAfter(outcome.remaining());
        record.setCreatedAt(Instant.now());

        try {
            idempotencyRepository.save(record);
        } catch (DataIntegrityViolationException raceOnSameKey) {
            return idempotencyRepository.findById(idempotencyKey)
                    .map(this::toOutcome)
                    .orElse(outcome);
        }

        return outcome;
    }

    private Outcome toOutcome(IdempotencyRecord record) {
        return new Outcome(record.isSuccess(), record.getRemainingAfter(), record.getErrorCode());
    }
}

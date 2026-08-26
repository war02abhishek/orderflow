package com.orderflow.inventory.service;

import com.orderflow.inventory.domain.IdempotencyRecord;
import com.orderflow.inventory.exception.SkuNotFoundException;
import com.orderflow.inventory.repo.IdempotencyRepository;
import com.orderflow.inventory.repo.StockRepository;
import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class InventoryService {

    private static final String RESERVE = "RESERVE";
    private static final String RELEASE = "RELEASE";
    private static final String INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK";
    private static final String REDIS_KEY_PREFIX = "idempotency:inventory:";
    private static final Duration REDIS_TTL = Duration.ofHours(24);

    private final StockRepository stockRepository;
    private final IdempotencyRepository idempotencyRepository;
    private final StringRedisTemplate redisTemplate;

    public record Outcome(boolean success, int remaining, String errorCode) {
    }

    @Transactional
    public Outcome reserve(String sku, int qty, String idempotencyKey) {
        return withIdempotency(idempotencyKey, sku, RESERVE, qty, () -> {
            // RETURNING folds the mutation and the read-back into one round
            // trip on the success path (Option A); a null result means the
            // WHERE clause matched no row (insufficient stock), so that one
            // still needs a separate read to report what's actually left.
            Integer remainingAfter = stockRepository.reserveAtomicReturning(sku, qty);
            return remainingAfter != null
                    ? new Outcome(true, remainingAfter, null)
                    : new Outcome(false, currentStock(sku), INSUFFICIENT_STOCK);
        });
    }

    @Transactional
    public Outcome release(String sku, int qty, String idempotencyKey) {
        return withIdempotency(idempotencyKey, sku, RELEASE, qty, () -> {
            Integer remainingAfter = stockRepository.releaseAtomicReturning(sku, qty);
            if (remainingAfter == null) {
                throw new SkuNotFoundException(sku);
            }
            return new Outcome(true, remainingAfter, null);
        });
    }

    public int currentStock(String sku) {
        return stockRepository.findById(sku)
                .orElseThrow(() -> new SkuNotFoundException(sku))
                .getAvailableQty();
    }

    /**
     * Flash-sale readiness pass (see README): a Redis fast-path sits in
     * front of the durable Postgres check (G2). A retry of an
     * already-seen key is answered straight from Redis, never touching
     * Postgres at all -- this is specifically what stops a retry storm
     * under load from compounding onto an already-stressed database. It
     * does not speed up a genuinely first-time request, which still needs
     * the full Postgres path below since the mutation itself has to
     * happen somewhere durable; Redis just remembers the answer afterward
     * so the *next* attempt at this exact key is free.
     */
    private Outcome withIdempotency(String idempotencyKey, String sku, String operation, int qty,
            java.util.function.Supplier<Outcome> action) {
        String redisKey = REDIS_KEY_PREFIX + operation + ":" + idempotencyKey;

        String cached = redisTemplate.opsForValue().get(redisKey);
        if (cached != null) {
            log.info("duplicate {} for key {} caught by Redis fast-path, Postgres not touched",
                    operation, idempotencyKey);
            return decodeOutcome(cached);
        }

        var existing = idempotencyRepository.findById(idempotencyKey);
        if (existing.isPresent()) {
            log.info("duplicate {} for key {} caught by Postgres backstop (Redis was cold), caching for next time",
                    operation, idempotencyKey);
            Outcome outcome = toOutcome(existing.get());
            cacheInRedis(redisKey, outcome);
            return outcome;
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
            outcome = idempotencyRepository.findById(idempotencyKey)
                    .map(this::toOutcome)
                    .orElse(outcome);
        }

        cacheInRedis(redisKey, outcome);
        return outcome;
    }

    private void cacheInRedis(String redisKey, Outcome outcome) {
        redisTemplate.opsForValue().set(redisKey, encodeOutcome(outcome), REDIS_TTL);
    }

    private Outcome toOutcome(IdempotencyRecord record) {
        return new Outcome(record.isSuccess(), record.getRemainingAfter(), record.getErrorCode());
    }

    private String encodeOutcome(Outcome outcome) {
        return outcome.success() + ":" + outcome.remaining() + ":"
                + (outcome.errorCode() == null ? "" : outcome.errorCode());
    }

    private Outcome decodeOutcome(String encoded) {
        String[] parts = encoded.split(":", 3);
        boolean success = Boolean.parseBoolean(parts[0]);
        int remaining = Integer.parseInt(parts[1]);
        String errorCode = (parts.length > 2 && !parts[2].isEmpty()) ? parts[2] : null;
        return new Outcome(success, remaining, errorCode);
    }
}

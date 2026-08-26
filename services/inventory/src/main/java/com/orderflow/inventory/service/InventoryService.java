package com.orderflow.inventory.service;

import com.orderflow.inventory.exception.SkuNotFoundException;
import com.orderflow.inventory.repo.StockRepository;
import java.time.Duration;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Flash-sale readiness pass, Option C: Redis is now the sole, atomic
 * authority for both the idempotency check and the stock decision (see
 * reserve_release.lua) -- Postgres is a durable mirror the
 * StockReconciler keeps in sync afterward, not a party to this call at
 * all. That's the trade this system made explicitly, with README
 * documenting the accepted risk: if Redis loses data before an entry
 * reconciles to Postgres, a reseed-from-Postgres can restore a stock count
 * higher than reality, reopening a narrow window where G1's no-overselling
 * guarantee could be violated. Frequent reconciliation and Redis AOF
 * persistence shrink that window; they don't erase it.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class InventoryService {

    private static final String RESERVE = "RESERVE";
    private static final String RELEASE = "RELEASE";
    private static final String INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK";
    private static final String IDEMPOTENCY_KEY_PREFIX = "idempotency:inventory:";
    private static final String STOCK_KEY_PREFIX = "stock:";
    private static final String STREAM_KEY = "stock:reconciliation";
    private static final Duration REDIS_TTL = Duration.ofHours(24);

    private final StockRepository stockRepository;
    private final StringRedisTemplate redisTemplate;
    private final RedisScript<String> reserveReleaseScript;

    public record Outcome(boolean success, int remaining, String errorCode) {
    }

    public Outcome reserve(String sku, int qty, String idempotencyKey) {
        return runScript(RESERVE, sku, qty, idempotencyKey);
    }

    public Outcome release(String sku, int qty, String idempotencyKey) {
        return runScript(RELEASE, sku, qty, idempotencyKey);
    }

    public int currentStock(String sku) {
        String value = redisTemplate.opsForValue().get(STOCK_KEY_PREFIX + sku);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return stockRepository.findById(sku)
                .orElseThrow(() -> new SkuNotFoundException(sku))
                .getAvailableQty();
    }

    private Outcome runScript(String operation, String sku, int qty, String idempotencyKey) {
        String idempotencyRedisKey = IDEMPOTENCY_KEY_PREFIX + operation + ":" + idempotencyKey;
        String stockRedisKey = STOCK_KEY_PREFIX + sku;

        String result = redisTemplate.execute(
                reserveReleaseScript,
                List.of(idempotencyRedisKey, stockRedisKey, STREAM_KEY),
                String.valueOf(qty), sku, String.valueOf(REDIS_TTL.toSeconds()), operation);

        return parseResult(result, sku, operation, idempotencyKey);
    }

    private Outcome parseResult(String result, String sku, String operation, String idempotencyKey) {
        if (result.startsWith("OK:")) {
            return new Outcome(true, Integer.parseInt(result.substring(3)), null);
        }
        if (result.startsWith("FAIL:")) {
            return new Outcome(false, Integer.parseInt(result.substring(5)), INSUFFICIENT_STOCK);
        }
        if (result.startsWith("ERROR:SKU_NOT_FOUND")) {
            throw new SkuNotFoundException(sku);
        }
        log.warn("unexpected reserve_release.lua result for {} {} key={}: {}",
                operation, sku, idempotencyKey, result);
        throw new IllegalStateException("unexpected Redis script result: " + result);
    }
}

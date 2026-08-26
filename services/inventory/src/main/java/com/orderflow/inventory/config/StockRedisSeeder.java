package com.orderflow.inventory.config;

import com.orderflow.inventory.repo.StockRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Seeds Redis's stock:{sku} counters from Postgres on boot -- using
 * setIfAbsent so a normal app restart never clobbers a live counter. This
 * only actually matters the first time a SKU is seen, or after Redis
 * itself has lost its data (a pod restart without persistence, or a true
 * data-loss event) -- see README's "Flash-sale readiness pass" section for
 * the risk window this seeding is part of managing: Postgres's value only
 * reflects what's been *reconciled* so far, so a reseed after Redis loses
 * unreconciled decisions restores a count that can be briefly too high.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StockRedisSeeder implements CommandLineRunner {

    private static final String STOCK_KEY_PREFIX = "stock:";

    private final StockRepository stockRepository;
    private final StringRedisTemplate redisTemplate;

    @Override
    public void run(String... args) {
        stockRepository.findAll().forEach(stock -> {
            String key = STOCK_KEY_PREFIX + stock.getSku();
            Boolean wasAbsent = redisTemplate.opsForValue()
                    .setIfAbsent(key, String.valueOf(stock.getAvailableQty()));
            if (Boolean.TRUE.equals(wasAbsent)) {
                log.info("seeded Redis stock counter for {} from Postgres: {}",
                        stock.getSku(), stock.getAvailableQty());
            }
        });
    }
}

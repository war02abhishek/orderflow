package com.orderflow.inventory.config;

import com.orderflow.inventory.domain.Stock;
import com.orderflow.inventory.repo.StockRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Seeds a couple of SKUs on first boot so the service is testable
 * immediately. SKU-FLASH-1 starts deliberately low so a concurrent-request
 * test can actually exhaust it (G1 verification).
 */
@Component
@RequiredArgsConstructor
public class StockSeeder implements CommandLineRunner {

    private final StockRepository stockRepository;

    @Override
    public void run(String... args) {
        if (stockRepository.count() > 0) {
            return;
        }
        stockRepository.save(new Stock("SKU-FLASH-1", 25));
        stockRepository.save(new Stock("SKU-STANDARD-1", 1000));
    }
}

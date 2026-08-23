package com.orderflow.inventory.repo;

import com.orderflow.inventory.domain.Stock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, String> {

    /**
     * Atomic conditional decrement (G1) — the WHERE clause is the whole
     * point: two concurrent requests racing the same SKU can't both read
     * "enough stock" and both write, because there's no read-then-write gap
     * for them to race in. Returns 0 rows affected when there isn't enough
     * stock left, instead of throwing.
     */
    @Modifying
    @Query("UPDATE Stock s SET s.availableQty = s.availableQty - :qty "
            + "WHERE s.sku = :sku AND s.availableQty >= :qty")
    int reserveAtomic(@Param("sku") String sku, @Param("qty") int qty);

    @Modifying
    @Query("UPDATE Stock s SET s.availableQty = s.availableQty + :qty WHERE s.sku = :sku")
    int releaseAtomic(@Param("sku") String sku, @Param("qty") int qty);
}

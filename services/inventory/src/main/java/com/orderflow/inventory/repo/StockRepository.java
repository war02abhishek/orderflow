package com.orderflow.inventory.repo;

import com.orderflow.inventory.domain.Stock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, String> {

    /**
     * Flash-sale readiness pass, Option C: the correctness decision (is
     * there enough stock, check-and-decrement atomically) now happens in
     * Redis (see reserve_release.lua) -- Postgres no longer makes that
     * decision at all. This is the reconciler's write path only
     * (StockReconciler): apply a delta Redis already decided was valid,
     * unconditionally. No WHERE availableQty >= qty guard here on
     * purpose -- re-checking against Postgres's (possibly lagging) view
     * would only produce a Postgres record that disagrees with what Redis
     * already told the client, which is worse than trusting the
     * reconciliation.
     */
    @Modifying
    @Query("UPDATE Stock s SET s.availableQty = s.availableQty + :delta WHERE s.sku = :sku")
    void applyDelta(@Param("sku") String sku, @Param("delta") int delta);
}

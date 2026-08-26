package com.orderflow.inventory.repo;

import com.orderflow.inventory.domain.Stock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, String> {

    /**
     * Atomic conditional decrement (G1) — the WHERE clause is the whole
     * point: two concurrent requests racing the same SKU can't both read
     * "enough stock" and both write, because there's no read-then-write gap
     * for them to race in. A null result means the WHERE clause matched no
     * row (not enough stock left), instead of throwing.
     *
     * Flash-sale readiness pass, Option A: RETURNING folds the mutation and
     * the read-back into one round trip instead of two separate calls.
     * Native query because JPQL has no RETURNING clause; no @Modifying
     * because Hibernate needs to treat this as result-producing, not
     * executeUpdate()-only, to actually see the returned column.
     */
    @Query(value = "UPDATE stock SET available_qty = available_qty - :qty "
            + "WHERE sku = :sku AND available_qty >= :qty RETURNING available_qty",
            nativeQuery = true)
    Integer reserveAtomicReturning(@Param("sku") String sku, @Param("qty") int qty);

    @Query(value = "UPDATE stock SET available_qty = available_qty + :qty "
            + "WHERE sku = :sku RETURNING available_qty",
            nativeQuery = true)
    Integer releaseAtomicReturning(@Param("sku") String sku, @Param("qty") int qty);
}

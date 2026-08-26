package com.orderflow.inventory.service;

import com.orderflow.inventory.domain.ReconciledEvent;
import com.orderflow.inventory.repo.ReconciledEventRepository;
import com.orderflow.inventory.repo.StockRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * A separate bean from StockReconciler on purpose: each stream entry needs
 * its own transaction (applied and committed before that specific entry is
 * ACKed in Redis), not one transaction for the whole batch -- if entry #50
 * failed, entries #1-49 must stay committed and ACKed, not roll back with
 * it. A same-class @Transactional method called via self-invocation
 * doesn't go through Spring's proxy at all (bit us once already, in
 * OutboxRelay); a genuinely separate bean is what makes per-entry
 * transactions actually apply here.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StockReconciliationWriter {

    private final StockRepository stockRepository;
    private final ReconciledEventRepository reconciledEventRepository;

    @Transactional
    public void applyToPostgres(MapRecord<String, Object, Object> record) {
        String entryId = record.getId().getValue();
        if (reconciledEventRepository.existsById(entryId)) {
            log.debug("stream entry {} already reconciled, skipping", entryId);
            return;
        }

        String sku = (String) record.getValue().get("sku");
        int qty = Integer.parseInt((String) record.getValue().get("qty"));
        String op = (String) record.getValue().get("op");
        int delta = "RESERVE".equals(op) ? -qty : qty;

        stockRepository.applyDelta(sku, delta);

        try {
            reconciledEventRepository.save(new ReconciledEvent(entryId, Instant.now()));
        } catch (DataIntegrityViolationException raceOnSameEntry) {
            log.debug("stream entry {} already reconciled by another replica", entryId);
        }
    }
}

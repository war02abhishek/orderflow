package com.orderflow.inventory.service;

import com.orderflow.inventory.support.PodInfo;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The only thing that ever writes to Postgres's stock table now (Option C).
 * Redis already made the correctness decision (reserve_release.lua); this
 * just durably mirrors that decision into Postgres, on its own schedule --
 * the same "committed vs. published" gap the Outbox pattern in orders
 * accepts, applied here to "decided in Redis vs. durably recorded." The
 * actual per-entry write lives in StockReconciliationWriter, a separate
 * bean so each entry's transaction is real (see that class for why).
 *
 * A unique consumer name per pod (the pod hostname) lets Redis Streams'
 * consumer-group load-balancing split reconciliation work across all
 * inventory replicas, the same idea CooperativeStickyAssignor gives Kafka
 * consumer groups elsewhere in this system.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StockReconciler {

    private static final String STREAM_KEY = "stock:reconciliation";
    private static final String GROUP = "inventory-reconciler";

    private final StringRedisTemplate redisTemplate;
    private final StockReconciliationWriter writer;

    @PostConstruct
    public void ensureConsumerGroup() {
        try {
            redisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0"), GROUP);
            log.info("created Redis Stream consumer group {} on {}", GROUP, STREAM_KEY);
        } catch (Exception e) {
            log.debug("consumer group {} already exists: {}", GROUP, e.getMessage());
        }
    }

    @Scheduled(fixedDelay = 300)
    public void reconcile() {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(GROUP, PodInfo.hostname()),
                StreamReadOptions.empty().count(100),
                StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));

        if (records == null || records.isEmpty()) {
            return;
        }

        for (MapRecord<String, Object, Object> record : records) {
            writer.applyToPostgres(record);
            redisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP, record.getId());
        }
    }

    /**
     * Without this, a consumer that dies mid-processing (pod killed after
     * XREADGROUP delivers an entry but before it's ACKed -- not rare in
     * this cluster, we've seen plenty of pod restarts) leaves that entry
     * permanently stuck in its pending list: ReadOffset.lastConsumed() in
     * reconcile() only ever sees genuinely new entries, never revisits an
     * old delivery. An orphaned entry means Postgres never learns about
     * that reservation, which quietly worsens the exact risk this whole
     * design already accepts -- a reseed-from-Postgres after Redis data
     * loss would then overstate availability by even more than the
     * documented window already allows for.
     *
     * Reclaims (XCLAIM) any entry idle for more than 10s under this pod's
     * own identity, then processes it exactly like a freshly-read one.
     */
    @Scheduled(fixedDelay = 5000)
    public void reclaimStaleEntries() {
        var pending = redisTemplate.opsForStream()
                .pending(STREAM_KEY, GROUP, Range.unbounded(), 100);
        if (pending == null || pending.isEmpty()) {
            return;
        }

        for (PendingMessage message : pending) {
            if (message.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofSeconds(10)) < 0) {
                continue;
            }
            List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream().claim(
                    STREAM_KEY, GROUP, PodInfo.hostname(),
                    XClaimOptions.minIdle(Duration.ofSeconds(10)).ids(message.getIdAsString()));

            for (MapRecord<String, Object, Object> record : claimed) {
                log.info("reclaimed stale stream entry {} (idle {}), reprocessing",
                        record.getId(), message.getElapsedTimeSinceLastDelivery());
                writer.applyToPostgres(record);
                redisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP, record.getId());
            }
        }
    }
}

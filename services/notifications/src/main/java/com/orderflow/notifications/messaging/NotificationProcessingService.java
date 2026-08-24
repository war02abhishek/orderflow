package com.orderflow.notifications.messaging;

import com.orderflow.notifications.domain.ProcessedEvent;
import com.orderflow.notifications.messaging.dto.OrderCancelledEvent;
import com.orderflow.notifications.messaging.dto.OrderPlacedEvent;
import com.orderflow.notifications.repo.ProcessedEventRepository;
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
public class NotificationProcessingService {

    private static final String REDIS_KEY_PREFIX = "idempotency:notifications:";
    private static final Duration REDIS_TTL = Duration.ofHours(24);

    private final ProcessedEventRepository processedEventRepository;
    private final StringRedisTemplate redisTemplate;

    @Transactional
    public void handleOrderPlaced(OrderPlacedEvent event) {
        if (!claim("order-placed:" + event.orderId())) {
            return;
        }
        log.info("confirmation sent for order {} ({} x{})", event.orderId(), event.sku(), event.quantity());
    }

    @Transactional
    public void handleOrderCancelled(OrderCancelledEvent event) {
        if (!claim("order-cancelled:" + event.orderId())) {
            return;
        }
        log.info("cancellation notice sent for order {} ({}): {}", event.orderId(), event.sku(), event.reason());
    }

    /** G17's two-tier idempotency check, same shape as payment's. */
    private boolean claim(String dedupeKey) {
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(REDIS_KEY_PREFIX + dedupeKey, "1", REDIS_TTL);
        if (Boolean.FALSE.equals(isNew)) {
            log.info("duplicate {} caught by Redis fast-path, skipping", dedupeKey);
            return false;
        }

        if (processedEventRepository.existsById(dedupeKey)) {
            log.info("duplicate {} caught by Postgres backstop, skipping", dedupeKey);
            return false;
        }

        try {
            processedEventRepository.save(new ProcessedEvent(dedupeKey, Instant.now()));
        } catch (DataIntegrityViolationException raceOnSameKey) {
            log.info("duplicate {} caught by unique-constraint race, skipping", dedupeKey);
            return false;
        }
        return true;
    }
}

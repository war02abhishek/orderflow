package com.orderflow.payment.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.payment.domain.ProcessedEvent;
import com.orderflow.payment.messaging.dto.PaymentCompletedEvent;
import com.orderflow.payment.messaging.dto.PaymentFailedEvent;
import com.orderflow.payment.messaging.dto.PaymentRequestedEvent;
import com.orderflow.payment.repo.ProcessedEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class PaymentProcessingService {

    private static final String REDIS_KEY_PREFIX = "idempotency:payment:";
    private static final Duration REDIS_TTL = Duration.ofHours(24);
    private static final String ORDER_ID_HEADER = "X-Order-Id";

    private final ProcessedEventRepository processedEventRepository;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final double failureRate;

    public PaymentProcessingService(
            ProcessedEventRepository processedEventRepository,
            StringRedisTemplate redisTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @org.springframework.beans.factory.annotation.Value("${payment.failure-rate:0.3}") double failureRate) {
        this.processedEventRepository = processedEventRepository;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.failureRate = failureRate;
    }

    /**
     * G17's two-tier idempotency check: Redis SETNX answers "seen before?"
     * in under a millisecond for the common case (including redelivery
     * right after a rebalance); the Postgres unique constraint is what
     * still catches it correctly if Redis lost the key to a restart.
     */
    @Transactional
    public void handle(PaymentRequestedEvent event) {
        String orderId = event.orderId();

        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(REDIS_KEY_PREFIX + orderId, "1", REDIS_TTL);
        if (Boolean.FALSE.equals(isNew)) {
            log.info("duplicate PaymentRequested for order {} caught by Redis fast-path, skipping", orderId);
            return;
        }

        if (processedEventRepository.existsById(orderId)) {
            log.info("duplicate PaymentRequested for order {} caught by Postgres backstop, skipping", orderId);
            return;
        }

        try {
            processedEventRepository.save(new ProcessedEvent(orderId, Instant.now()));
        } catch (DataIntegrityViolationException raceOnSameOrder) {
            log.info("duplicate PaymentRequested for order {} caught by unique-constraint race, skipping", orderId);
            return;
        }

        boolean approved = ThreadLocalRandom.current().nextDouble() >= failureRate;
        if (approved) {
            publish("payment-completed", orderId, new PaymentCompletedEvent(orderId));
        } else {
            publish("payment-failed", orderId, new PaymentFailedEvent(orderId, "simulated decline"));
        }
    }

    private void publish(String topic, String orderId, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            var message = MessageBuilder.withPayload(json)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.KEY, orderId)
                    .setHeader(ORDER_ID_HEADER, orderId)
                    .build();
            kafkaTemplate.send(message);
        } catch (Exception e) {
            throw new RuntimeException("failed to publish to " + topic + " for order " + orderId, e);
        }
    }
}

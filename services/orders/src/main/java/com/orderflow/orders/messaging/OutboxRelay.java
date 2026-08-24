package com.orderflow.orders.messaging;

import com.orderflow.orders.domain.OutboxEvent;
import com.orderflow.orders.repo.OutboxEventRepository;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The only thing in the system that actually talks to Kafka on behalf of an
 * order-state change. OrdersService and SagaResultListener never publish
 * directly -- they write an OutboxEvent row in the same local transaction
 * as the state change, and this poller picks it up afterward. That gap
 * between "committed" and "published" is fine to have, because nothing
 * downstream depends on sub-second delivery; what matters is that the gap
 * can never silently drop or duplicate an event relative to the DB state.
 *
 * One transactional boundary per batch (not one per row via a self-invoked
 * helper) -- a same-class method call doesn't go through Spring's
 * transaction proxy, so splitting this into "outer poll" + "inner
 * @Transactional publishOne" would silently run everything in whatever
 * transaction (or none) the outer method started.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

    private static final String ORDER_ID_HEADER = "X-Order-Id";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishUnpublished() {
        List<OutboxEvent> batch = outboxEventRepository.findTop50ByPublishedFalseOrderByCreatedAtAsc();
        for (OutboxEvent event : batch) {
            var message = MessageBuilder.withPayload(event.getPayload())
                    .setHeader(KafkaHeaders.TOPIC, event.getTopic())
                    .setHeader(KafkaHeaders.KEY, event.getOrderId())
                    .setHeader(ORDER_ID_HEADER, event.getOrderId())
                    .build();

            kafkaTemplate.send(message);

            event.setPublished(true);
            event.setPublishedAt(Instant.now());
            outboxEventRepository.save(event);
            log.info("outbox: published {} for order {}", event.getTopic(), event.getOrderId());
        }
    }
}

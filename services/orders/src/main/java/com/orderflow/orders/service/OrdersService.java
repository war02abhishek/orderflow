package com.orderflow.orders.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.orders.client.InventoryClient;
import com.orderflow.orders.domain.Order;
import com.orderflow.orders.domain.OrderStatus;
import com.orderflow.orders.domain.OutboxEvent;
import com.orderflow.orders.exception.OrderNotFoundException;
import com.orderflow.orders.messaging.dto.OrderCancelledEvent;
import com.orderflow.orders.messaging.dto.OrderPlacedEvent;
import com.orderflow.orders.messaging.dto.PaymentRequestedEvent;
import com.orderflow.orders.repo.OrderRepository;
import com.orderflow.orders.repo.OutboxEventRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrdersService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final InventoryClient inventoryClient;
    private final ObjectMapper objectMapper;

    /**
     * The one synchronous precondition (no saga involved): reserve stock,
     * then either reject immediately or move into the saga as
     * AWAITING_PAYMENT. The order row and the PaymentRequested outbox row
     * commit in the same transaction -- that's the entire Outbox guarantee.
     */
    @Transactional
    public Order checkout(String sku, int quantity) {
        var reservation = inventoryClient.reserve(sku, quantity);

        if (!reservation.success()) {
            return orderRepository.save(new Order(sku, quantity, OrderStatus.REJECTED));
        }

        Order order = orderRepository.save(new Order(sku, quantity, OrderStatus.AWAITING_PAYMENT));
        writeOutbox(order.getId().toString(), "payment-requested",
                new PaymentRequestedEvent(order.getId().toString(), sku, quantity));
        return order;
    }

    /**
     * Success path. Guarded by the current status: a redelivered
     * PaymentCompleted (post-rebalance, before this consumer's offset
     * committed the first time) finds the order already CONFIRMED and
     * no-ops instead of writing a second OrderPlaced outbox row. This is
     * orders' own idempotency mechanism -- a state-machine guard, not a
     * separate dedup store, since "already past this state" is exactly
     * what a state machine is for.
     */
    @Transactional
    public void confirm(String orderId) {
        Order order = orderRepository.findById(UUID.fromString(orderId))
                .orElseThrow(() -> new OrderNotFoundException(UUID.fromString(orderId)));

        if (order.getStatus() != OrderStatus.AWAITING_PAYMENT) {
            log.info("order {} already past AWAITING_PAYMENT (status={}), ignoring redelivered PaymentCompleted",
                    orderId, order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.CONFIRMED);
        orderRepository.save(order);
        writeOutbox(orderId, "order-placed",
                new OrderPlacedEvent(orderId, order.getSku(), order.getQuantity()));
    }

    /**
     * Failure path: the compensating transaction. Same idempotency guard as
     * confirm() -- a redelivered PaymentFailed for an already-CANCELLED
     * order no-ops instead of releasing stock a second time.
     */
    @Transactional
    public void cancel(String orderId, String reason) {
        Order order = orderRepository.findById(UUID.fromString(orderId))
                .orElseThrow(() -> new OrderNotFoundException(UUID.fromString(orderId)));

        if (order.getStatus() != OrderStatus.AWAITING_PAYMENT) {
            log.info("order {} already past AWAITING_PAYMENT (status={}), ignoring redelivered PaymentFailed",
                    orderId, order.getStatus());
            return;
        }

        inventoryClient.release(order.getSku(), order.getQuantity());

        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);
        writeOutbox(orderId, "order-cancelled",
                new OrderCancelledEvent(orderId, order.getSku(), order.getQuantity(), reason));
    }

    public Order get(UUID id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    private void writeOutbox(String orderId, String topic, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            outboxEventRepository.save(new OutboxEvent(orderId, topic, json));
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize outbox payload for order " + orderId, e);
        }
    }
}

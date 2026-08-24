package com.orderflow.orders.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.orders.messaging.dto.PaymentCompletedEvent;
import com.orderflow.orders.messaging.dto.PaymentFailedEvent;
import com.orderflow.orders.service.OrdersService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SagaResultListener {

    private final OrdersService ordersService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "payment-completed", groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentCompleted(String payload) throws Exception {
        var event = objectMapper.readValue(payload, PaymentCompletedEvent.class);
        ordersService.confirm(event.orderId());
    }

    @KafkaListener(topics = "payment-failed", groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentFailed(String payload) throws Exception {
        var event = objectMapper.readValue(payload, PaymentFailedEvent.class);
        ordersService.cancel(event.orderId(), event.reason());
    }
}

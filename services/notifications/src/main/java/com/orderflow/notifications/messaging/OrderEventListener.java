package com.orderflow.notifications.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.notifications.messaging.dto.OrderCancelledEvent;
import com.orderflow.notifications.messaging.dto.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final NotificationProcessingService processingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order-placed", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(String payload) throws Exception {
        processingService.handleOrderPlaced(objectMapper.readValue(payload, OrderPlacedEvent.class));
    }

    @KafkaListener(topics = "order-cancelled", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(String payload) throws Exception {
        processingService.handleOrderCancelled(objectMapper.readValue(payload, OrderCancelledEvent.class));
    }
}

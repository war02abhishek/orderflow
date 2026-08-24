package com.orderflow.payment.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.payment.messaging.dto.PaymentRequestedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentRequestListener {

    private final PaymentProcessingService processingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "payment-requested", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) throws Exception {
        PaymentRequestedEvent event = objectMapper.readValue(payload, PaymentRequestedEvent.class);
        processingService.handle(event);
    }
}

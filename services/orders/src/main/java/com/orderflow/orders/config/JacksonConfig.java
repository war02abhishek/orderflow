package com.orderflow.orders.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring MVC's HttpMessageConverter builds its own internal ObjectMapper,
 * which is why REST endpoints work without this -- but nothing registers
 * one as an injectable bean, which is what the outbox relay and saga
 * listener need for manual JSON (de)serialization.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

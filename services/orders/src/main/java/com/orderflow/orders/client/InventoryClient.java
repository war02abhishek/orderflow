package com.orderflow.orders.client;

import com.orderflow.orders.client.dto.InventoryReserveResponse;
import com.orderflow.orders.client.dto.QuantityRequest;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
@RequiredArgsConstructor
public class InventoryClient {

    private final RestClient inventoryRestClient;

    public record ReserveResult(boolean success, int remaining, String errorCode) {
    }

    /**
     * One Idempotency-Key is generated per call and sent as a header on the
     * single outbound request. If Istio's retry policy (Phase 4) resends
     * this exact request at the network level, it resends the same header
     * with it — inventory (G2) then replays its stored result instead of
     * reserving twice. No retry-aware logic is needed here.
     */
    public ReserveResult reserve(String sku, int qty) {
        String idempotencyKey = UUID.randomUUID().toString();

        InventoryReserveResponse response = inventoryRestClient.post()
                .uri("/stock/{sku}/reserve", sku)
                .header("Idempotency-Key", idempotencyKey)
                .contentType(MediaType.APPLICATION_JSON)
                .body(new QuantityRequest(qty))
                .exchange((request, resp) -> resp.bodyTo(InventoryReserveResponse.class));

        return new ReserveResult(response.success(), response.remaining(), response.errorCode());
    }
}

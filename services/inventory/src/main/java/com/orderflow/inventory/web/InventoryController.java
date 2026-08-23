package com.orderflow.inventory.web;

import com.orderflow.inventory.service.InventoryService;
import com.orderflow.inventory.support.PodInfo;
import com.orderflow.inventory.web.dto.QuantityRequest;
import com.orderflow.inventory.web.dto.ReleaseResponse;
import com.orderflow.inventory.web.dto.ReserveResponse;
import com.orderflow.inventory.web.dto.StockView;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stock")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    @GetMapping("/{sku}")
    public StockView get(@PathVariable String sku) {
        return new StockView(sku, inventoryService.currentStock(sku), PodInfo.hostname());
    }

    @PostMapping("/{sku}/reserve")
    public ResponseEntity<ReserveResponse> reserve(
            @PathVariable String sku,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody QuantityRequest request) {

        var outcome = inventoryService.reserve(sku, request.quantity(), idempotencyKey);
        var body = new ReserveResponse(
                sku, request.quantity(), outcome.remaining(), outcome.success(),
                outcome.errorCode(), PodInfo.hostname());

        return outcome.success()
                ? ResponseEntity.ok(body)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    @PostMapping("/{sku}/release")
    public ResponseEntity<ReleaseResponse> release(
            @PathVariable String sku,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody QuantityRequest request) {

        var outcome = inventoryService.release(sku, request.quantity(), idempotencyKey);
        return ResponseEntity.ok(new ReleaseResponse(
                sku, request.quantity(), outcome.remaining(), PodInfo.hostname()));
    }
}

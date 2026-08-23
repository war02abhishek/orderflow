package com.orderflow.inventory.exception;

public class SkuNotFoundException extends RuntimeException {

    public SkuNotFoundException(String sku) {
        super("no such SKU: " + sku);
    }
}

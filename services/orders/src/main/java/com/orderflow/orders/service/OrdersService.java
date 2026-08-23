package com.orderflow.orders.service;

import com.orderflow.orders.client.InventoryClient;
import com.orderflow.orders.domain.Order;
import com.orderflow.orders.domain.OrderStatus;
import com.orderflow.orders.exception.OrderNotFoundException;
import com.orderflow.orders.repo.OrderRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrdersService {

    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;

    @Transactional
    public Order checkout(String sku, int quantity) {
        var reservation = inventoryClient.reserve(sku, quantity);
        OrderStatus status = reservation.success() ? OrderStatus.RESERVED : OrderStatus.REJECTED;
        return orderRepository.save(new Order(sku, quantity, status));
    }

    public Order get(UUID id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}

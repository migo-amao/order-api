package com.eventsourcing.order.web;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/public/orders")
public class OrderCommandController {
    private final OrderService orderService;

    public OrderCommandController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping()
    public ResponseEntity<String> newOrder(@RequestBody Order order) {
        String orderId = orderService.createOrder(order);
        return ResponseEntity.status(201).body(orderId);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> cancelOrder(@PathVariable String id) {
        orderService.cancelOrder(id);
        return ResponseEntity.ok().build();
    }
}
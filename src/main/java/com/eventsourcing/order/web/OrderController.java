package com.eventsourcing.order.web;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.repository.OrderEvent;
import com.eventsourcing.order.repository.OrderEventRepository;
import com.eventsourcing.order.service.OrderService;
import com.eventsourcing.payment.repository.PaymentEvent;
import com.eventsourcing.payment.repository.PaymentEventRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/public/orders")
public class OrderController {
    private final OrderService orderService;
    private final OrderEventRepository orderEventRepository;
    private final PaymentEventRepository paymentEventRepository;

    public OrderController(OrderService orderService, OrderEventRepository orderEventRepository, PaymentEventRepository paymentEventRepository) {
        this.orderService = orderService;
        this.orderEventRepository = orderEventRepository;
        this.paymentEventRepository = paymentEventRepository;
    }

    @GetMapping("/{id}")
    public OrderVo getOrder(@PathVariable String id) {
        Order order = orderService.getOrder(id);
        List<OrderEvent> orderEvents = orderEventRepository.findTopByOrderIdOrderByEventAtDesc(id);
        List<PaymentEvent> paymentEvents = paymentEventRepository.findTopByOrderIdOrderByEventAtDesc(id);

        return new OrderVo(order, orderEvents.getFirst(), paymentEvents.getFirst());
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
package com.eventsourcing.order.web;

import com.eventsourcing.order.domain.Order;
import com.eventsourcing.order.repository.OrderRepository;
import com.eventsourcing.payment.repository.Payment;
import com.eventsourcing.payment.repository.PaymentRepository;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {
    private final PaymentRepository paymentRepository;
    private final OrderRepository orderRepository;

    public OrderQueryController(PaymentRepository paymentRepository, OrderRepository orderRepository) {
        this.paymentRepository = paymentRepository;
        this.orderRepository = orderRepository;
    }

    @PreAuthorize("hasRole('USER')")
    @GetMapping("/{id}")
    public OrderVo getOrder(@PathVariable String id) {
        List<Order> orders = orderRepository.findTopByOrderIdOrderByAtDesc(id);
        List<Payment> payments = paymentRepository.findTopByOrderIdOrderByAtDesc(id);
        Order order = orders.isEmpty() ? null : orders.getFirst();
        Payment payment = payments.isEmpty() ? null : payments.getFirst();
        return new OrderVo(order, payment);
    }

    @PreAuthorize("hasRole('USER')")
    @GetMapping
    public List<OrderVo> getOrders(@RequestParam String userId) {
        List<OrderVo> orderVos = new ArrayList<>();
        List<Order> orders = orderRepository.findAllByUserId(userId);
        if (!orders.isEmpty()) {
            Map<String, List<Order>> orderMap = orders.stream().collect(Collectors.groupingBy(Order::getOrderId));
            for (Map.Entry<String, List<Order>> entry : orderMap.entrySet()) {
                Order order = entry.getValue().stream().sorted(Comparator.comparing(Order::getAt).reversed()).toList().getFirst();
                List<Payment> paymentList = paymentRepository.findTopByOrderIdOrderByAtDesc(order.getOrderId());

                Payment payment = paymentList.isEmpty() ? null : paymentList.getFirst();
                orderVos.add(new OrderVo(order, payment));
            }
        }

        return orderVos;
    }
}

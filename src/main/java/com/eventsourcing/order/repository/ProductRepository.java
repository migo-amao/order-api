package com.eventsourcing.order.repository;

import com.eventsourcing.order.domain.Product;
import com.eventsourcing.order.domain.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, String> {}
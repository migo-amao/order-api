package com.eventsourcing.order.repository;

import com.eventsourcing.order.domain.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, String> {}
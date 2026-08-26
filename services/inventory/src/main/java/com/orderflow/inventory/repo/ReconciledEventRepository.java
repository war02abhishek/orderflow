package com.orderflow.inventory.repo;

import com.orderflow.inventory.domain.ReconciledEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReconciledEventRepository extends JpaRepository<ReconciledEvent, String> {
}

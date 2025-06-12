package com.example.dataprocessor.repository;

import com.example.dataprocessor.model.SalesData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SalesDataRepository extends JpaRepository<SalesData, Long> {
} 
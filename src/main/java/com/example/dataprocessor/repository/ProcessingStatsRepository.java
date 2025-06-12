package com.example.dataprocessor.repository;

import com.example.dataprocessor.model.FileProcessingStatus;
import com.example.dataprocessor.model.ProcessingStats;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProcessingStatsRepository extends JpaRepository<ProcessingStats, Long> {
    Optional<ProcessingStats> findByFileProcessingStatus(FileProcessingStatus fileProcessingStatus);
} 
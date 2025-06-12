package com.example.dataprocessor.repository;

import com.example.dataprocessor.model.FileProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FileProcessingStatusRepository extends JpaRepository<FileProcessingStatus, Long> {
} 
package com.example.dataprocessor.service;

import com.example.dataprocessor.model.ProcessingStats;
import com.example.dataprocessor.repository.ProcessingStatsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProcessingStatsService {

    @Autowired
    private ProcessingStatsRepository processingStatsRepository;

    public List<ProcessingStats> getAllProcessingStats() {
        return processingStatsRepository.findAll();
    }
} 
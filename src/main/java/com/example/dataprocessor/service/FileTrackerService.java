package com.example.dataprocessor.service;

import com.example.dataprocessor.model.FileProcessingStatus;
import com.example.dataprocessor.model.ProcessingStats;
import com.example.dataprocessor.repository.FileProcessingStatusRepository;
import com.example.dataprocessor.repository.ProcessingStatsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class FileTrackerService {

    @Autowired
    private FileProcessingStatusRepository fileProcessingStatusRepository;

    @Autowired
    private ProcessingStatsRepository processingStatsRepository;

    public FileProcessingStatus createFileProcessingStatus(String filename, String originalFilename) {
        FileProcessingStatus status = new FileProcessingStatus();
        status.setFilename(filename);
        status.setOriginalFilename(originalFilename);
        status.setUploadTime(LocalDateTime.now());
        status.setStatus("PENDING");
        return fileProcessingStatusRepository.save(status);
    }

    public void updateFileStatus(Long fileId, String status, String errorMessage) {
        Optional<FileProcessingStatus> optionalStatus = fileProcessingStatusRepository.findById(fileId);
        optionalStatus.ifPresent(s -> {
            s.setStatus(status);
            s.setProcessTime(LocalDateTime.now());
            s.setErrorMessage(errorMessage);
            fileProcessingStatusRepository.save(s);
        });
    }

    public void updateProcessingStats(FileProcessingStatus fileStatus, int totalRows, int processedRows, int failedRows, long durationSeconds) {
        ProcessingStats stats = processingStatsRepository.findByFileProcessingStatus(fileStatus)
                .orElse(new ProcessingStats());

        stats.setFileProcessingStatus(fileStatus);
        stats.setTotalRows(totalRows);
        stats.setProcessedRows(processedRows);
        stats.setFailedRows(failedRows);
        stats.setProcessingDurationSeconds(java.math.BigDecimal.valueOf(durationSeconds));
        processingStatsRepository.save(stats);
    }

    public Optional<FileProcessingStatus> getFileStatusById(Long fileId) {
        return fileProcessingStatusRepository.findById(fileId);
    }

    public Optional<ProcessingStats> getProcessingStatsByFileId(Long fileId) {
        return fileProcessingStatusRepository.findById(fileId)
                .flatMap(processingStatsRepository::findByFileProcessingStatus);
    }
} 
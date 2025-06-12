package com.example.dataprocessor.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;

@Entity
@Table(name = "processing_stats")
public class ProcessingStats {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne
    @JoinColumn(name = "file_id", referencedColumnName = "id")
    private FileProcessingStatus fileProcessingStatus;

    private Integer totalRows;
    private Integer processedRows;
    private Integer failedRows;
    private BigDecimal processingDurationSeconds;

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public FileProcessingStatus getFileProcessingStatus() {
        return fileProcessingStatus;
    }

    public void setFileProcessingStatus(FileProcessingStatus fileProcessingStatus) {
        this.fileProcessingStatus = fileProcessingStatus;
    }

    public Integer getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(Integer totalRows) {
        this.totalRows = totalRows;
    }

    public Integer getProcessedRows() {
        return processedRows;
    }

    public void setProcessedRows(Integer processedRows) {
        this.processedRows = processedRows;
    }

    public Integer getFailedRows() {
        return failedRows;
    }

    public void setFailedRows(Integer failedRows) {
        this.failedRows = failedRows;
    }

    public BigDecimal getProcessingDurationSeconds() {
        return processingDurationSeconds;
    }

    public void setProcessingDurationSeconds(BigDecimal processingDurationSeconds) {
        this.processingDurationSeconds = processingDurationSeconds;
    }
} 
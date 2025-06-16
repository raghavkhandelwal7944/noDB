package com.example.dataprocessor.controller;

import com.example.dataprocessor.model.FileProcessingStatus;
import com.example.dataprocessor.model.ProcessingStats;
import com.example.dataprocessor.service.FileProcessorService;
import com.example.dataprocessor.service.FileTrackerService;
import com.example.dataprocessor.service.ProcessingStatsService;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api")
@EnableScheduling
public class FileUploadController {

    @Autowired
    private FileProcessorService fileProcessorService;

    @Autowired
    private FileTrackerService fileTrackerService;

    @Autowired
    private ProcessingStatsService processingStatsService;

    @PostMapping("/upload/large-file")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("Please select a file to upload.");
        }

        FileProcessingStatus status = fileTrackerService.createFileProcessingStatus(file.getOriginalFilename(), file.getOriginalFilename());
        status.setStatus("PROCESSING"); // Set status to processing immediately
        fileTrackerService.updateFileStatus(status.getId(), "PROCESSING", null);

        try {
            // Call the async service method directly
            fileProcessorService.processFile(file, status.getId());
            // The status update to COMPLETED/FAILED will be handled by FileProcessorService
        } catch (IOException | CsvValidationException | InterruptedException | ExecutionException e) {
            fileTrackerService.updateFileStatus(status.getId(), "FAILED", e.getMessage());
            System.err.println("File processing failed for " + file.getOriginalFilename() + ": " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("File upload failed: " + e.getMessage());
        }

        return ResponseEntity.ok("File upload initiated. Tracking ID: " + status.getId());
    }

    @GetMapping("/file-status/{fileId}")
    public ResponseEntity<FileProcessingStatus> getFileStatus(@PathVariable Long fileId) {
        Optional<FileProcessingStatus> status = fileTrackerService.getFileStatusById(fileId);
        return status.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/processing-stats")
    public ResponseEntity<List<ProcessingStats>> getAllProcessingStats() {
        List<ProcessingStats> stats = processingStatsService.getAllProcessingStats();
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/")
    public ResponseEntity<String> healthCheck() {
        return new ResponseEntity<>("Service is up and running!", HttpStatus.OK);
    }
} 
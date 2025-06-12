package com.example.dataprocessor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ScheduledTasksService {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledTasksService.class);

    @Autowired
    private FileTrackerService fileTrackerService;

    // Commenting out scheduled tasks for now
    /*
    @Scheduled(fixedRate = 60000) // Run every minute
    public void processPendingFiles() {
        logger.info("Running scheduled task: processPendingFiles at {}", java.time.LocalDateTime.now());
        fileTrackerService.processPendingFiles();
    }

    @Scheduled(cron = "0 0 0 * * ?") // Run at midnight every day
    public void cleanupOldFiles() {
        logger.info("Running scheduled task: cleanupOldFiles at {}", java.time.LocalDateTime.now());
        fileTrackerService.cleanupOldFiles();
    }
    */
} 
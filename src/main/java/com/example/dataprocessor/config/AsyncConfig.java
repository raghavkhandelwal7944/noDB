package com.example.dataprocessor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "taskExecutor")
    public ExecutorService taskExecutor() {
        return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }
} 
package com.eventflow.eventservice.config;

import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class OutboxExecutorConfig {
    @Bean(name = "rabbitOutboxExecutor")
    ThreadPoolTaskExecutor rabbitOutboxExecutor(
            @Value("$" + "{eventflow.outbox.rabbit-workers:4}") int workers,
            @Value("$" + "{eventflow.outbox.shutdown-wait-seconds:10}") int shutdownWaitSeconds) {
        return executor("eventflow-rabbit-outbox-", workers, shutdownWaitSeconds);
    }

    @Bean(name = "kafkaOutboxExecutor")
    ThreadPoolTaskExecutor kafkaOutboxExecutor(
            @Value("$" + "{eventflow.outbox.kafka-workers:2}") int workers,
            @Value("$" + "{eventflow.outbox.shutdown-wait-seconds:10}") int shutdownWaitSeconds) {
        return executor("eventflow-kafka-outbox-", workers, shutdownWaitSeconds);
    }

    private ThreadPoolTaskExecutor executor(String threadNamePrefix, int configuredWorkers, int shutdownWaitSeconds) {
        int workers = Math.max(1, configuredWorkers);
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(workers);
        executor.setMaxPoolSize(workers);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(Math.max(1, shutdownWaitSeconds));
        executor.initialize();
        return executor;
    }
}


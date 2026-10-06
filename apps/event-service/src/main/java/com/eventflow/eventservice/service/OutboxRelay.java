package com.eventflow.eventservice.service;

import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.eventflow.eventservice.metrics.OutboxMetrics;

@Component
public class OutboxRelay {
    private final OutboxDeliveryStore deliveryStore;
    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String kafkaTopic;
    private final OutboxMetrics metrics;
    private final int maxAttempts;
    private final long baseDelayMs;
    private final long maxDelayMs;
    private final double jitterRatio;
    private final long leaseMs;
    private final ThreadPoolTaskExecutor rabbitExecutor;
    private final ThreadPoolTaskExecutor kafkaExecutor;
    private final Semaphore rabbitPermits;
    private final Semaphore kafkaPermits;

    public OutboxRelay(
            OutboxDeliveryStore deliveryStore,
            RabbitTemplate rabbitTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("$" + "{eventflow.kafka.topic:eventflow.domain-events}") String kafkaTopic,
            OutboxMetrics metrics,
            @Value("$" + "{eventflow.outbox.max-attempts:8}") int maxAttempts,
            @Value("$" + "{eventflow.outbox.base-delay-ms:250}") long baseDelayMs,
            @Value("$" + "{eventflow.outbox.max-delay-ms:300000}") long maxDelayMs,
            @Value("$" + "{eventflow.outbox.jitter-ratio:0.2}") double jitterRatio,
            @Value("$" + "{eventflow.outbox.lease-ms:30000}") long leaseMs,
            @Qualifier("rabbitOutboxExecutor") ThreadPoolTaskExecutor rabbitExecutor,
            @Qualifier("kafkaOutboxExecutor") ThreadPoolTaskExecutor kafkaExecutor) {
        this.deliveryStore = deliveryStore;
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTopic = kafkaTopic;
        this.metrics = metrics;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.baseDelayMs = Math.max(1, baseDelayMs);
        this.maxDelayMs = Math.max(this.baseDelayMs, maxDelayMs);
        this.jitterRatio = Math.max(0, Math.min(1, jitterRatio));
        this.leaseMs = Math.max(1, leaseMs);
        this.rabbitExecutor = rabbitExecutor;
        this.kafkaExecutor = kafkaExecutor;
        this.rabbitPermits = new Semaphore(rabbitExecutor.getMaxPoolSize(), true);
        this.kafkaPermits = new Semaphore(kafkaExecutor.getMaxPoolSize(), true);
    }

    @Scheduled(fixedDelayString = "$" + "{eventflow.outbox.poll-ms:1000}")
    public void relay() {
        deliveryStore.recoverExpiredLeases(maxAttempts);
        deliveryStore.quarantineExhausted(maxAttempts);

        dispatch("RABBIT", rabbitExecutor, rabbitPermits);
        dispatch("KAFKA", kafkaExecutor, kafkaPermits);
    }

    private void dispatch(String channel, ThreadPoolTaskExecutor executor, Semaphore permits) {
        while (permits.tryAcquire()) {
            OutboxDeliveryStore.ClaimedOutbox row;
            try {
                row = deliveryStore.claim(channel, maxAttempts, leaseMs, UUID.randomUUID());
            } catch (RuntimeException exception) {
                permits.release();
                throw exception;
            }

            if (row == null) {
                permits.release();
                return;
            }

            try {
                executor.execute(() -> deliver(row, permits));
            } catch (RejectedExecutionException exception) {
                int released = deliveryStore.releaseClaim(row);
                if (released == 0) metrics.markClaimConflict();
                permits.release();
            }
        }
    }

    private void deliver(OutboxDeliveryStore.ClaimedOutbox row, Semaphore permits) {
        try {
            publish(row);
            int finalized = deliveryStore.markSent(row);
            if (finalized == 1) {
                metrics.markSent();
            } else {
                metrics.markClaimConflict();
            }
        } catch (Exception exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            metrics.markFailed();
            String error = trim(exception.getMessage());
            int updated = deliveryStore.retryOrQuarantine(
                    row, error, maxAttempts, backoffMs(row.attempts()));
            if (updated == 0) {
                metrics.markClaimConflict();
            } else if (row.attempts() >= maxAttempts) {
                metrics.markQuarantined();
            }
        } finally {
            permits.release();
        }
    }

    private void publish(OutboxDeliveryStore.ClaimedOutbox row) throws Exception {
        if ("RABBIT".equals(row.channel())) {
            CorrelationData correlation = new CorrelationData(row.id().toString());
            rabbitTemplate.convertAndSend("eventflow.events", row.eventType(), row.payload(), correlation);
            var confirm = correlation.getFuture().get(5, TimeUnit.SECONDS);
            if (!confirm.isAck()) {
                throw new IllegalStateException("Rabbit publish was not confirmed: " + confirm.getReason());
            }
            if (correlation.getReturned() != null) {
                throw new IllegalStateException("Rabbit publish was returned: " + correlation.getReturned().getReplyText());
            }
            return;
        }

        if ("KAFKA".equals(row.channel())) {
            kafkaTemplate.send(kafkaTopic, row.aggregateId().toString(), row.payload()).get(5, TimeUnit.SECONDS);
            return;
        }

        throw new IllegalArgumentException("Unsupported outbox channel: " + row.channel());
    }

    private long backoffMs(int attempt) {
        long delay = baseDelayMs;
        for (int i = 1; i < attempt; i++) {
            delay = delay >= maxDelayMs / 2 ? maxDelayMs : Math.min(maxDelayMs, delay * 2);
        }
        long jitter = (long) (delay * jitterRatio);
        if (jitter == 0) return delay;
        long sampled = ThreadLocalRandom.current().nextLong(-jitter, jitter + 1);
        return Math.max(1, Math.min(maxDelayMs, delay + sampled));
    }

    private String trim(String value) {
        if (value == null) return "unknown";
        return value.length() > 500 ? value.substring(0, 500) : value;
    }
}

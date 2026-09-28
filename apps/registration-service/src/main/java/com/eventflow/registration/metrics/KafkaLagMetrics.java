package com.eventflow.registration.metrics;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import jakarta.annotation.PreDestroy;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.OffsetSpec;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class KafkaLagMetrics {
    private final AdminClient adminClient;
    private final String topic;
    private final String consumerGroup;
    private final AtomicLong lag = new AtomicLong(-1);
    private final AtomicLong lastRefreshEpoch = new AtomicLong(0);

    public KafkaLagMetrics(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${eventflow.kafka.topic:eventflow.domain-events}") String topic,
            @Value("${spring.kafka.consumer.group-id:eventflow-analytics}") String consumerGroup,
            MeterRegistry registry) {
        this.adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        Gauge.builder("eventflow.kafka.consumer.lag", lag, AtomicLong::doubleValue)
                .description("Total consumer lag for the analytics group; -1 means unavailable")
                .register(registry);
        Gauge.builder("eventflow.kafka.consumer.lag.last.refresh.epoch", lastRefreshEpoch, AtomicLong::doubleValue)
                .description("Epoch seconds of the last successful Kafka lag refresh")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${eventflow.metrics.kafka-lag-refresh-ms:5000}",
               initialDelayString = "${eventflow.metrics.kafka-lag-refresh-ms:5000}")
    public void refresh() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                    .listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> latestRequests = committed.keySet().stream()
                    .filter(partition -> partition.topic().equals(topic))
                    .collect(Collectors.toMap(partition -> partition, partition -> OffsetSpec.latest()));
            if (latestRequests.isEmpty()) {
                lag.set(0);
                lastRefreshEpoch.set(System.currentTimeMillis() / 1000);
                return;
            }
            Map<TopicPartition, Long> latest = adminClient.listOffsets(latestRequests)
                    .all().get(5, TimeUnit.SECONDS)
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
            long total = committed.entrySet().stream()
                    .filter(entry -> latest.containsKey(entry.getKey()))
                    .mapToLong(entry -> Math.max(0, latest.get(entry.getKey()) - entry.getValue().offset()))
                    .sum();
            lag.set(total);
            lastRefreshEpoch.set(System.currentTimeMillis() / 1000);
        } catch (Exception exception) {
            lag.set(-1);
        }
    }

    @PreDestroy
    void close() {
        adminClient.close(Duration.ofSeconds(2));
    }
}

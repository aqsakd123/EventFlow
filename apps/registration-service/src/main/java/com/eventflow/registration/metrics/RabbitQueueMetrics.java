package com.eventflow.registration.metrics;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RabbitQueueMetrics {
    private static final String QUEUE = "eventflow.registration.events";

    private final RabbitAdmin admin;
    private final AtomicLong ready = new AtomicLong(-1);
    private final AtomicLong consumers = new AtomicLong(-1);

    public RabbitQueueMetrics(ConnectionFactory connectionFactory, MeterRegistry registry) {
        this.admin = new RabbitAdmin(connectionFactory);
        Gauge.builder("eventflow.rabbit.registration.queue.ready", ready, AtomicLong::doubleValue)
                .description("Messages ready in the registration projection queue; -1 means unavailable")
                .register(registry);
        Gauge.builder("eventflow.rabbit.registration.queue.consumers", consumers, AtomicLong::doubleValue)
                .description("Consumers attached to the registration projection queue")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${eventflow.metrics.refresh-ms:5000}",
               initialDelayString = "${eventflow.metrics.refresh-ms:5000}")
    public void refresh() {
        try {
            Properties properties = admin.getQueueProperties(QUEUE);
            if (properties == null) {
                ready.set(-1);
                consumers.set(-1);
                return;
            }
            ready.set(number(properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT)));
            consumers.set(number(properties.get(RabbitAdmin.QUEUE_CONSUMER_COUNT)));
        } catch (RuntimeException exception) {
            ready.set(-1);
            consumers.set(-1);
        }
    }

    private long number(Object value) {
        return value instanceof Number number ? number.longValue() : -1;
    }
}

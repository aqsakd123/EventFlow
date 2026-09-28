package com.eventflow.registration.config;

import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.interceptor.RetryInterceptorBuilder;

@Configuration
public class RabbitConfig {
    @Bean
    DirectExchange eventExchange() { return new DirectExchange("eventflow.events", true, false); }

    @Bean
    DirectExchange deadLetterExchange() { return new DirectExchange("eventflow.events.dlx", true, false); }

    @Bean
    Queue registrationQueue() {
        return new Queue("eventflow.registration.events", true, false, false,
                Map.of("x-dead-letter-exchange", "eventflow.events.dlx", "x-dead-letter-routing-key", "registration.dead"));
    }

    @Bean
    Queue registrationDlq() { return new Queue("eventflow.registration.events.dlq", true); }

    @Bean
    Binding registrationBinding(Queue registrationQueue, DirectExchange eventExchange) {
        return BindingBuilder.bind(registrationQueue).to(eventExchange).with("EVENT_PUBLISHED");
    }

    @Bean
    Binding registrationUpdatedBinding(Queue registrationQueue, DirectExchange eventExchange) {
        return BindingBuilder.bind(registrationQueue).to(eventExchange).with("EVENT_UPDATED");
    }

    @Bean
    Binding registrationCancelledBinding(Queue registrationQueue, DirectExchange eventExchange) {
        return BindingBuilder.bind(registrationQueue).to(eventExchange).with("EVENT_CANCELLED");
    }

    @Bean
    Binding registrationEndedBinding(Queue registrationQueue, DirectExchange eventExchange) {
        return BindingBuilder.bind(registrationQueue).to(eventExchange).with("EVENT_ENDED");
    }

    @Bean
    Binding registrationReconciledBinding(Queue registrationQueue, DirectExchange eventExchange) {
        return BindingBuilder.bind(registrationQueue).to(eventExchange).with("EVENT_RECONCILED");
    }

    @Bean
    Binding registrationDeadLetterBinding(Queue registrationDlq, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(registrationDlq).to(deadLetterExchange).with("registration.dead");
    }

    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

    @Bean(name = "rabbitListenerContainerFactory")
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        var factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setDefaultRequeueRejected(false);
        factory.setAdviceChain(RetryInterceptorBuilder.stateless()
                .maxAttempts(3)
                .backOffOptions(250, 2.0, 2_000)
                .build());
        return factory;
    }
}

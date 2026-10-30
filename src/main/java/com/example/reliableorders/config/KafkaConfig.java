package com.example.reliableorders.config;

import com.example.reliableorders.exception.RetryableProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Instant;

@Configuration
public class KafkaConfig {

    public static final String ORDERS_TOPIC = "orders-topic";
    public static final String DLQ_TOPIC = "orders-topic-dlq";

    @Bean
    public org.apache.kafka.clients.admin.NewTopic ordersTopic() {
        return TopicBuilder.name(ORDERS_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic ordersDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<Object, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(DLQ_TOPIC, record.partition()));
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners(new RetryLoggingListener());
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    private static final class RetryLoggingListener implements RetryListener {
        @Override
        public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception, int deliveryAttempt) {
            System.out.printf("[%s] Retry attempt %d for event on %s-%d: %s%n",
                    Instant.now(), deliveryAttempt, record.topic(), record.partition(), exception.getMessage());
        }

        @Override
        public void recovered(ConsumerRecord<?, ?> record, Exception exception) {
            System.out.printf("[%s] Retries exhausted for event on %s-%d; publishing to DLQ%n",
                    Instant.now(), record.topic(), record.partition());
        }
    }
}

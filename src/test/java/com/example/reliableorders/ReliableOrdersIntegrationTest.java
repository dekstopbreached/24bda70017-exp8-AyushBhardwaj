package com.example.reliableorders;

import com.example.reliableorders.config.KafkaConfig;
import com.example.reliableorders.kafka.OrderProducer;
import com.example.reliableorders.model.OrderEvent;
import com.example.reliableorders.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {KafkaConfig.ORDERS_TOPIC, KafkaConfig.DLQ_TOPIC})
class ReliableOrdersIntegrationTest {

    @Autowired
    private OrderProducer producer;

    @Autowired
    private ProcessedEventRepository repository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @BeforeEach
    void clearDatabase() {
        repository.deleteAll();
    }

    @Test
    void successfulEventIsProcessedOnce() throws Exception {
        String eventId = UUID.randomUUID().toString();
        producer.send(event(eventId, "order-success", "Alice", BigDecimal.TEN)).get();

        awaitRepositoryCount(1);
        assertThat(repository.existsById(eventId)).isTrue();
    }

    @Test
    void failingEventIsRetriedAndPublishedToDlq() throws Exception {
        String eventId = UUID.randomUUID().toString();
        try (Consumer<String, OrderEvent> dlqConsumer = createDlqConsumer()) {
            producer.send(event(eventId, "order-fail", "FAIL", BigDecimal.ONE)).get();
            ConsumerRecord<String, OrderEvent> record = KafkaTestUtils.getSingleRecord(
                    dlqConsumer, KafkaConfig.DLQ_TOPIC, Duration.ofSeconds(30));
            assertThat(record.value().getEventId()).isEqualTo(eventId);
            assertThat(repository.existsById(eventId)).isFalse();
        }
    }

    @Test
    void duplicateEventIdIsProcessedOnlyOnce() throws Exception {
        String eventId = UUID.randomUUID().toString();
        OrderEvent event = event(eventId, "order-duplicate", "Bob", BigDecimal.ONE);
        producer.send(event);
        producer.send(event);

        awaitRepositoryCount(1);
        assertThat(repository.count()).isEqualTo(1);
    }

    private Consumer<String, OrderEvent> createDlqConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "test-dlq-" + UUID.randomUUID(), "false", embeddedKafka);
        Consumer<String, OrderEvent> consumer = new DefaultKafkaConsumerFactory<String, OrderEvent>(
                props, new StringDeserializer(),
                new org.springframework.kafka.support.serializer.JsonDeserializer<>(OrderEvent.class, false))
                .createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, KafkaConfig.DLQ_TOPIC);
        return consumer;
    }

    private void awaitRepositoryCount(long expected) throws InterruptedException {
        for (int i = 0; i < 100 && repository.count() < expected; i++) {
            Thread.sleep(100);
        }
    }

    private OrderEvent event(String id, String orderId, String customer, BigDecimal amount) {
        return new OrderEvent(id, orderId, customer, amount, "CREATED");
    }
}

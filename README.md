# Reliable Orders - Spring Kafka Demo

This project demonstrates three reliability techniques in an event-driven Spring Boot application:

- exponential retry backoff for transient processing failures;
- dead-letter publishing after retries are exhausted; and
- database-backed idempotency for duplicate delivery.

## Requirements

- Java 17 or newer
- Maven 3.8+
- Docker Desktop with Docker Compose

## Run it

Start Kafka, Zookeeper, and Kafka UI:

```bash
docker compose up -d
```

Kafka UI is available at http://localhost:8090. The application creates both three-partition topics when it starts.

Start Spring Boot:

```bash
mvn spring-boot:run
```

Run the integration tests without Docker:

```bash
mvn test
```

## Try the scenarios

### Successful order

```bash
curl -X POST http://localhost:8080/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"orderId\":\"order-100\",\"customerName\":\"Alice\",\"amount\":49.99,\"status\":\"CREATED\"}"
```

Expected response status is `202 Accepted`, followed by a log similar to:

```text
Processing order: eventId=<generated-uuid>, orderId=order-100, amount=49.99
```

The event ID is stored in H2 and the event is processed once.

### Retry and DLQ

```bash
curl -X POST http://localhost:8080/api/orders/fail ^
  -H "Content-Type: application/json" ^
  -d "{\"orderId\":\"order-200\",\"customerName\":\"any\",\"amount\":10,\"status\":\"CREATED\"}"
```

The endpoint changes the customer name to `FAIL`, which causes a retryable exception:

```text
[2026-10-30T...] Retry attempt 1 ... Simulated processing failure ...
[2026-10-30T...] Retry attempt 2 ... Simulated processing failure ...
[2026-10-30T...] Retry attempt 3 ... Simulated processing failure ...
[2026-10-30T...] Retry attempt 4 ... Simulated processing failure ...
[2026-10-30T...] Retries exhausted ... publishing to DLQ
DLQ message received: eventId=<generated-uuid>, orderId=order-200, partition=0, customerName=FAIL
```

The delays between retry attempts are approximately 1, 2, 4, and 8 seconds (bounded by 10 seconds). The recoverer keeps the original partition and adds standard Spring Kafka exception and original-record headers, including the exception message, stack trace, topic, and partition.

### Duplicate event

```bash
curl -X POST http://localhost:8080/api/orders/duplicate ^
  -H "Content-Type: application/json" ^
  -d "{\"orderId\":\"order-300\",\"customerName\":\"Bob\",\"amount\":25,\"status\":\"CREATED\"}"
```

The endpoint sends the same event ID twice. Logs show one processing operation and:

```text
Duplicate event ignored: eventId=<same-uuid>
```

## Observation

- **Retry:** the `DefaultErrorHandler` uses `ExponentialBackOffWithMaxRetries` with four retries, a 1-second initial delay, multiplier 2.0, and a 10-second maximum. Timestamped retry logs make the increasing delays visible.
- **DLQ:** after the retry budget is exhausted, `DeadLetterPublishingRecoverer` sends the original record to `orders-topic-dlq` using its original partition. The DLQ monitor logs the failed event and its metadata.
- **Idempotency:** `ProcessedEvent.eventId` is the primary key. The consumer checks it before processing and saves it in the same transaction after successful processing. A concurrent insert that wins the unique constraint is treated as a duplicate.

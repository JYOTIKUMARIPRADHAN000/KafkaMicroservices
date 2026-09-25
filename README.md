# Kafka Microservices - E-Commerce Order Processing

A practical **Spring Boot + Apache Kafka** microservices project demonstrating event-driven communication, Kafka partitions, consumer groups, consumer scaling, ordering, offset handling, retry, idempotency, and Dead Letter Topic (DLT).

---

## 1. Project Overview

This project simulates an e-commerce order-processing workflow using four independent microservices.

### Services

| Service | Responsibility |
|---|---|
| Order Service | Creates orders and publishes `order-created` |
| Payment Service | Processes payment and publishes payment result |
| Delivery Service | Creates delivery after successful payment |
| Notification Service | Consumes events and sends notifications |

### Flow

`Order Service → order-created → Payment Service → payment-success/payment-failed`

`payment-success → Delivery Service → delivery-created → Notification Service`

`payment-failed → Notification Service`

---

## 2. Technology Stack

- Java
- Spring Boot
- Spring Kafka
- Apache Kafka
- Docker
- Maven
- Jackson
- REST API
- Git / GitHub
- Eclipse / IntelliJ IDEA

---

## 3. Kafka Topics

| Topic | Producer | Consumers | Purpose |
|---|---|---|---|
| `order-created` | Order Service | Payment, Notification | Order creation |
| `payment-success` | Payment Service | Delivery, Notification | Successful payment |
| `payment-failed` | Payment Service | Notification | Failed payment |
| `delivery-created` | Delivery Service | Notification | Delivery creation |

### Retry / DLT Topics

```text
payment-success
       ↓
payment-success-retry
       ↓
payment-success-dlt
```

---

## 4. Partition Configuration

| Topic | Partitions |
|---|---:|
| `order-created` | 3 |
| `payment-success` | 2 |
| `payment-failed` | 2 |
| `delivery-created` | 5 |

### Check Topic

```cmd
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic order-created
```

---

## 5. Service Details

### Order Service

- Creates and stores orders.
- Creates `ORDER_CREATED` events.
- Publishes events to `order-created`.
- Uses `orderId` as the Kafka message key.

### Payment Service

- Consumes `order-created`.
- Processes payment.
- Publishes `PAYMENT_SUCCESS` to `payment-success`.
- Publishes `PAYMENT_FAILED` to `payment-failed`.
- Demonstrates idempotency and retry/DLT.

### Delivery Service

- Consumes `payment-success`.
- Creates delivery information.
- Publishes `DELIVERY_CREATED` to `delivery-created`.
- Demonstrates retry and DLT.

### Notification Service

Consumes:

- `order-created`
- `payment-success`
- `payment-failed`
- `delivery-created`

It generates the corresponding notification for each event.

---

## 6. End-to-End Flow

### Successful Payment

```text
Order Created
     ↓
order-created
     ↓
Payment Service
     ↓
payment-success
     ↓
Delivery Service
     ↓
delivery-created
     ↓
Notification Service
```

### Failed Payment

```text
Order Created
     ↓
order-created
     ↓
Payment Service
     ↓
payment-failed
     ↓
Notification Service
```

When payment fails, Delivery Service does not create a delivery because it consumes only `payment-success`.

---

## 7. Consumer Groups

| Service | Consumer Group |
|---|---|
| Payment Service | `payment-service` |
| Delivery Service | `delivery-service` |
| Notification Service | `notification-service` |

### Check Consumer Group

```cmd
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group payment-service
```

Different consumer groups can independently consume the same Kafka event.

---

## 8. Consumer Scaling

The `order-created` topic has **3 partitions**.

Multiple Payment Service instances can run with the same:

```text
groupId = payment-service
```

Example:

```text
order-created
 P0    P1    P2
 |     |     |
 C1    C2    C3
```

### Important Rule

```text
Maximum active consumers <= Number of partitions
```

Therefore, with 3 partitions, up to 3 consumers can actively consume partitions within the same consumer group.

---

## 9. Ordering

Kafka guarantees ordering **within a partition**.

This project uses:

```text
Kafka Key = orderId
```

Therefore, events for the same `orderId` are routed consistently to the same partition.

```text
Same orderId
     ↓
Same Kafka key
     ↓
Same partition
     ↓
Ordering preserved
```

Kafka does not guarantee global ordering across different partitions.

---

## 10. Offset Handling

Every Kafka record has an offset within its partition.

Example:

```text
Partition 0

Offset 100 → Event A
Offset 101 → Event B
Offset 102 → Event C
```

Offsets are **partition-specific**.

Check consumer offsets:

```cmd
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group payment-service
```

The application also logs:

```text
Topic
Partition
Offset
Key
Message
```

When a consumer restarts, Kafka uses the committed offset for the consumer group and partition.

---

## 11. Retry Mechanism

Spring Kafka `@RetryableTopic` is used for retry processing.

Example:

```java
@RetryableTopic(attempts = "3")
@KafkaListener(
    topics = "payment-success",
    groupId = "delivery-service"
)
public void consumePaymentSuccess(
        ConsumerRecord<String, String> record) {
    
    // processing
}
```

### Retry Flow

```text
payment-success
      ↓
Delivery Service
      ↓
Processing Failure
      ↓
payment-success-retry
      ↓
Retry Attempts
      ↓
payment-success-dlt
```

---

## 12. Idempotency

The Payment Service demonstrates duplicate-event handling using `eventId`.

```java
private final Set<String> processedEvents =
        ConcurrentHashMap.newKeySet();
```

Before processing:

```java
String eventId = event.getEventId();

if (!processedEvents.add(eventId)) {
    System.out.println(
        "DUPLICATE EVENT DETECTED - Skipping eventId: "
        + eventId
    );
    return;
}
```

### Result

```text
First Event
    ↓
Process

Duplicate Event
    ↓
Detect eventId
    ↓
Skip
```

> The current in-memory implementation is for demonstration. A production system should use durable storage such as a database or Redis.

---

## 13. Dead Letter Topic (DLT)

A DLT stores a message when processing repeatedly fails and configured retries are exhausted.

### DLT Flow

```text
payment-success
      ↓
Delivery Service
      ↓
Processing Failure
      ↓
payment-success-retry
      ↓
Retries Exhausted
      ↓
payment-success-dlt
      ↓
@DltHandler
```

### Demo Failure Condition

For testing, Delivery Service can intentionally fail orders from `60` to `65`:

```java
if (event.getOrderId() >= 60 &&
    event.getOrderId() <= 65) {

    throw new RuntimeException(
        "Simulated delivery processing failure"
    );
}
```

Example:

```text
Order 59 → Success
Order 60 → Retry → DLT
Order 61 → Retry → DLT
Order 62 → Retry → DLT
...
Order 65 → Retry → DLT
Order 66 → Success
```

This condition is only for demonstration/testing.

---

## 14. Prerequisites

Install:

- Java 17+
- Maven
- Docker Desktop
- Git
- Eclipse / IntelliJ IDEA
- Postman

Verify:

```cmd
java -version
mvn -version
docker --version
git --version
```

---

## 15. Kafka / Docker Setup

### Start Zookeeper

```cmd
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest
```

### Start Kafka

```cmd
docker run -d --name kafka -p 9092:9092 ^
-e KAFKA_BROKER_ID=1 ^
-e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 ^
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 ^
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 ^
confluentinc/cp-kafka:7.5.0
```

### Check Containers

```cmd
docker ps
```

### Check Kafka Logs

```cmd
docker logs kafka
```

### List Topics

```cmd
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 16. How to Run Each Service

Clone the repository:

```cmd
git clone https://github.com/JYOTIKUMARIPRADHAN000/KafkaMicroservices.git
```

Go to the project:

```cmd
cd KafkaMicroservices
```

### Project Structure

```text
KafkaMicroservices/
├── kafkaOrderService/
├── kafkaPaymentService/
├── kafkaDeliveryService/
└── kafkaNotificationService/
```

### Recommended Startup Order

```text
1. Kafka
2. Order Service
3. Payment Service
4. Delivery Service
5. Notification Service
```

### Run Order Service

```cmd
cd kafkaOrderService
mvn spring-boot:run
```

### Run Payment Service

```cmd
cd kafkaPaymentService
mvn spring-boot:run
```

### Run Delivery Service

```cmd
cd kafkaDeliveryService
mvn spring-boot:run
```

### Run Notification Service

```cmd
cd kafkaNotificationService
mvn spring-boot:run
```

The services can also be run directly from Eclipse or IntelliJ as Spring Boot applications.

---

## 17. API Examples

> Use the actual port configured in the Order Service `application.properties`.

### Create Order

```http
POST http://localhost:8080/order
Content-Type: application/json
```

Request:

```json
{
  "customerId": 31472,
  "customerName": "Rahul",
  "productId": 7821,
  "productName": "Dinner Set",
  "quantity": 2,
  "amount": 12500,
  "deliveryAddress": "Bangalore"
}
```

### Successful Payment

Current logic:

```text
amount <= 50000 → PAYMENT_SUCCESS
```

Example:

```json
{
  "customerId": 62891,
  "customerName": "Priya",
  "productId": 9634,
  "productName": "Premium Furniture Set",
  "quantity": 1,
  "amount": 45000,
  "deliveryAddress": "Mumbai"
}
```

Expected:

```text
payment-success
    ↓
Delivery Service
    ↓
delivery-created
```

### Failed Payment

Current logic:

```text
amount > 50000 → PAYMENT_FAILED
```

Example:

```json
{
  "customerId": 62891,
  "customerName": "Priya",
  "productId": 9634,
  "productName": "Premium Furniture Set",
  "quantity": 1,
  "amount": 75000,
  "deliveryAddress": "Mumbai"
}
```

Expected:

```text
payment-failed
    ↓
Notification Service
```

### Duplicate Event Test

Temporary testing endpoint:

```http
POST http://localhost:<ORDER_SERVICE_PORT>/order/test-duplicate-event
```

Example:

```json
{
  "amount": 5000,
  "customerId": 92201,
  "deliveryAddress": "Mumbai",
  "eventId": "EVT-DUP-002",
  "eventType": "ORDER_CREATED",
  "orderId": 1000
}
```

Expected:

```text
First event
    ↓
Processed

Duplicate event
    ↓
DUPLICATE EVENT DETECTED
    ↓
Skipped
```

---

## 18. Testing Kafka Features

### Partitions

```cmd
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic order-created
```

### Consumer Groups

```cmd
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group payment-service
```

### DLT

List topics:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Consume DLT:

```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-success-dlt --from-beginning
```

### Expected Kafka Features

- ✅ Multiple Consumer Groups
- ✅ Partitions
- ✅ Ordering
- ✅ Consumer Scaling
- ✅ Offset Handling
- ✅ Retry Mechanism
- ✅ Duplicate Message Handling
- ✅ Idempotency
- ✅ Dead Letter Topic
- ✅ Asynchronous Communication

---

## Repository

**GitHub:**  
https://github.com/JYOTIKUMARIPRADHAN000/KafkaMicroservices

# EventGuard — Spring Boot + Aiven Kafka MVP

EventGuard accepts business events over REST, publishes them to Kafka, consumes them asynchronously, persists processing state in PostgreSQL, detects duplicate and late events, and exposes event status and anomaly APIs.

## Prerequisites
- Java 21+
- Maven 3.9+
- Docker Desktop
- An Aiven for Apache Kafka service with TLS enabled

## 1. Start PostgreSQL
```bash
docker compose up -d postgres
```

## 2. Configure Aiven Kafka
1. Create an Aiven for Apache Kafka service and wait until it is running.
2. In the service Overview, copy the bootstrap/server address and port.
3. Ensure the five topics below can be created by the configured Kafka user, or create them manually in Aiven Console.
4. Export the connection settings in your terminal. Do not commit credentials.

PowerShell example:
```powershell
# Aiven Console -> your service -> Overview: bootstrap server
$env:KAFKA_BOOTSTRAP_SERVERS="YOUR_HOST:YOUR_PORT"
$env:KAFKA_SECURITY_PROTOCOL="SASL_SSL"
$env:KAFKA_SASL_MECHANISM="SCRAM-SHA-512"
$env:KAFKA_SASL_USERNAME="YOUR_KAFKA_USERNAME"
$env:KAFKA_SASL_PASSWORD="YOUR_KAFKA_PASSWORD"
$env:DB_URL="jdbc:postgresql://localhost:5432/eventguard"
$env:DB_USERNAME="YOUR_USERNAME"
$env:DB_PASSWORD="YOUR_PASSWORD"
$env:KAFKA_SASL_MECHANISM=SCRAM-SHA-512
#Download your certs file and run below command to generate its .p12 file
keytool -import -trustcacerts -noprompt \
  -alias aiven-ca \
  -file certificate.pem \
  -keystore truststore.p12 \
  -storetype PKCS12 \
  -storepass your_password

$env:KAFKA_SSL_TRUSTSTORE_LOCATION=cert/client.truststore.p12
$env:KAFKA_SSL_TRUSTSTORE_PASSWORD=YOUR_PASSWORD
$env:KAFKA_CONSUMER_GROUP=eventguard-processor
```

The application defaults to SASL/SCRAM-SHA-512 over TLS. Set the SASL username and password from the Aiven connection details; do not commit credentials. The configured PKCS12 trust store is used for TLS. Do not disable certificate verification in production.

## 3. Run the application
```bash
mvn spring-boot:run
```
The Kafka admin client will attempt to create these topics (2 partitions each):
- `business-events`
- `validated-events`
- `anomaly-events`
- `processing-retry`
- `processing-dlq`

Aiven permissions and service plan limits may require creating topics manually. This design uses only five topics.

## 4. Submit an event
```bash
curl -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId":"evt-1001",
    "entityId":"order-9001",
    "eventType":"ORDER_CREATED",
    "amount":129.99,
    "occurredAt":"2026-10-20T12:00:00Z",
    "source":"checkout-api"
  }'
```
The API returns `202 Accepted` after submitting the message. Consumer processing is asynchronous.

## 5. Check status and anomalies
```bash
curl http://localhost:8080/api/v1/events/evt-1001
curl http://localhost:8080/api/v1/anomalies
curl http://localhost:8080/actuator/health
```
Send the same event ID again to exercise duplicate detection. Send an event whose `occurredAt` is more than 24 hours old to exercise late-event detection.

## Current API
| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/events` | Publish a business event |
| GET | `/api/v1/events/{eventId}` | Read processing status |
| GET | `/api/v1/anomalies` | List the latest 100 anomaly records |
| GET | `/actuator/health` | Application health |

## Architecture notes and next steps
- Event key is `entityId`, so events for one entity are routed to the same partition; this helps preserve per-entity order.
- Producer uses `acks=all` and Kafka producer idempotence.
- Consumer retries transient failures twice after the first attempt and then publishes to `processing-dlq`.
- Database uniqueness on `event_id` protects against concurrent duplicate deliveries.
- Kafka publication and PostgreSQL persistence are not one atomic transaction in this MVP. For stronger guarantees, implement a transactional outbox and an outbox relay before treating the platform as production-ready.
- `processing-retry` is reserved for the next iteration's scheduled retry workflow; the current error handler retries in place before routing to the DLQ.
- Anomaly persistence and event publication are also not atomically coordinated yet; the outbox pattern should cover these outputs too.
- Add authentication, authorization, request rate limits, retention policies, migrations (Flyway), and integration tests before production deployment.

## Suggested implementation milestones
1. Add Flyway migrations and service-level integration tests.
2. Add transactional outbox with polling relay and publish status.
3. Implement scheduled retry topic handling with retry count and original topic/partition/offset metadata.
4. Add Prometheus/Grafana dashboards and load tests; report measured throughput and p95/p99 latency.
5. Add schema versioning and compatibility checks.

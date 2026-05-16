# mini-fabric

Spring Boot + Kafka demo covering ordering, retries, DLQ, poison messages, consumer lag, schema evolution, and SASL/ACL security — all in one runnable stack.

## What this is

A small "people fabric" service used as a playground to observe real Kafka production patterns in isolation. Each pattern has a dedicated simulation endpoint and a matching script under `scripts/` to trigger and observe it end-to-end.

## Stack

| Layer | Technology |
|---|---|
| Language | Java 21, Spring Boot 4 |
| Messaging | Confluent Kafka 8.2  |
| Serialization | Confluent Avro + Schema Registry |
| Storage | MongoDB 7 |
| Infra | Docker Compose (local), Kubernetes manifests (k8s/) |

## Services

| Service | Port | Role |
|---|---|---|
| `profile-api` | 8080 | REST gateway + Kafka producer |
| `profile-consumer` | — | Main consumer with retry and rebalance logging |
| `dlq-consumer` | — | Reads DLQ, persists failed events to MongoDB |
| `schema-migrator` | 8083 | Registers Avro schemas, checks compatibility |
| `common` | — | Avro schema definitions (v1 and v2) |

## Kafka topics

| Topic | Partitions | Purpose |
|---|---|---|
| `profile.events` | 6 | Main topic, keyed by `profileId` |
| `profile.events-retry-0` | 6 | First retry (2s backoff) |
| `profile.events-retry-1` | 6 | Second retry (4s backoff) |
| `profile.events.dlq` | 3 | Dead letter queue |

## Security model

Kafka runs with SASL/PLAIN on both listeners and a StandardAuthorizer enforcing ACLs.

| Principal | Permissions |
|---|---|
| `profileapi` | WRITE + DESCRIBE on `profile.events` |
| `profileconsumer` | READ from all `profile.events*` topics; WRITE to retry and DLQ topics |
| `dlqconsumer` | READ from `profile.events.dlq` |

## Running locally

**Prerequisites:** Docker, Java 21, Maven

```bash
# Build all modules
mvn clean package -DskipTests

# Start infrastructure (Kafka, Schema Registry, MongoDB, UIs)
docker compose up kafka schema-registry mongodb kafka-ui mongo-express

# Run services in IntelliJ (or individually with java -jar)
# profile-api, profile-consumer, dlq-consumer, schema-migrator
```

**UI endpoints:**

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Mongo Express | http://localhost:8091 |
| Profile API | http://localhost:8080 |
| Schema Migrator | http://localhost:8083 |

## Patterns & how to trigger them

### Ordering
Messages are keyed by `profileId` — all events for the same profile always land on the same partition.

```bash
curl -X POST http://localhost:8080/profiles \
  -H "Content-Type: application/json" \
  -d '{"profileId":"p1","name":"Alice"}'
```

### Consumer lag
```bash
bash scripts/02-simulate-lag.sh 500
# Or set SIMULATE_SLOW_MS=500 on profile-consumer to make lag grow visibly
# Watch: Kafka UI → Consumer Groups → profile-consumer-group → lag per partition
```

### Poison message → retry → DLQ
```bash
bash scripts/03-inject-poison.sh
# Timeline:
#   t+0s  profile-consumer: attempt 1 → FAIL
#   t+2s  profile-consumer: attempt 2 on retry-0 → FAIL
#   t+6s  profile-consumer: attempt 3 on retry-1 → FAIL
#   t+6s  dlq-consumer: persists to MongoDB failed_events
```

### Rebalance
```bash
bash scripts/04-trigger-rebalance.sh
# Or: docker compose up --scale profile-consumer=3
# Watch profile-consumer logs for REBALANCE: partitions ASSIGNED/REVOKED
```

### Schema evolution
```bash
bash scripts/05-schema-evolution-demo.sh
# Registers v1 and v2 schemas, checks BACKWARD compatibility,
# then sends a v2-only MERGED event — v1 consumers read it as UPDATED
```

## Project structure

```
my-fabric/
├── common/                  # Avro schemas (v1, v2) + generated Java classes
├── profile-api/             # REST API + Kafka producer
├── profile-consumer/        # Main consumer, @RetryableTopic, rebalance hooks
├── dlq-consumer/            # DLQ reader → MongoDB
├── schema-migrator/         # Schema Registry client
├── scripts/                 # Demo scripts for each pattern
├── k8s/                     # Kubernetes manifests
└── docker-compose.yml       # Local infra stack
```

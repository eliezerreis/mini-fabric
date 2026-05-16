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

## Known issues & solutions

### 1. Consumer Lag
Consumers can't keep up with producer throughput.

| Solution | Side effects |
|---|---|
| Increase `concurrency` (more threads) | Limited by number of partitions — can't have more consumers than partitions |
| Increase partitions | Requires rebalance, ordering guarantee breaks across new partition layout |
| Optimize consumer processing (batch, async DB writes) | More complex code, harder to reason about failures |
| Scale horizontally (more consumer instances) | Same partition limit applies |

---

### 2. Ordering
Events for the same profile must be processed in order (CREATED → UPDATED → DELETED).

| Solution | Side effects |
|---|---|
| **Key-based routing** (current — `profileId` as key) | Ordering guaranteed per partition only |
| `max.in.flight.requests.per.connection=1` + `enable.idempotence=true` (already set) | Slightly reduces throughput |
| Forcing a specific partition | Breaks key-based routing, manual management burden |

> **Risk**: increasing partitions on an existing topic may reroute existing keys to different partitions, mixing old and new events out of order.

---

### 3. Poison Messages
A message that can't be deserialized or processed, causing the consumer to crash in a loop.

| Solution | Side effects |
|---|---|
| **`ErrorHandlingDeserializer`** (current) | Wraps deserialization errors, passes bad message downstream instead of crashing |
| **`@RetryableTopic`** (current) | Retries N times then routes to DLQ automatically |
| Dead-letter manually in catch block | More control but boilerplate, easy to miss edge cases |
| Skip and log | Simplest, but silent data loss |

> **Current gap**: poison detection relies on `schemaVersion == -1` check in `ProfileEventProcessor` — only catches deliberately injected poison, not all malformed messages.

---

### 4. Rebalance
Partition reassignment triggered by a consumer joining or leaving the group, causing a processing pause.

| Solution | Side effects |
|---|---|
| **`ConsumerSeekAware`** (current) | Logs assigned/revoked partitions, allows custom seek on rebalance |
| Cooperative sticky rebalance (`CooperativeStickyAssignor`) | Minimizes partitions moved, reduces pause window — requires all consumers in group to use it |
| Static membership (`group.instance.id`) | Consumer keeps partitions across restarts without rebalance, but stale members hold partitions until session timeout |
| Minimize processing time per message | Reduces chance of session timeout triggering rebalance |

---

### 5. Schema Evolution
Producers and consumers may run different schema versions simultaneously during rolling deploys.

| Solution | Side effects |
|---|---|
| **Backward compatibility** (current — new fields have defaults) | Old consumers read new messages safely, but can't access new fields |
| Forward compatibility | New consumers read old messages, producers must never remove fields |
| Full compatibility | Both directions safe, most restrictive — can only add optional fields |
| Schema Registry enforcement | Rejects incompatible schemas at registration time before reaching the topic |

> **Current setup**: v1 consumers read v2 messages safely because all new fields have defaults. `MERGED` enum defaults to `UPDATED` for v1 consumers — processed silently as an update.

---

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

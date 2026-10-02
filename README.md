# ciap-kafka — Transcript Processing Service

Spring Boot **Kafka + JPA** service for the Call Intelligence Platform (CIP). It
accepts telephony events/transcripts over **HTTP and WebSocket**, persists them to
PostgreSQL, publishes them to Kafka, and — on consuming them back — forwards the
results into `ciap-api` over REST.

Each concern (WebSocket ingestion, HTTP producer, Kafka consumer) is an independently
**toggle-able role**, so the same jar can run as separate WS / producer / consumer
nodes, or all-in-one — see [Deployment roles](#deployment-roles). It fully replaces
the Node `ciap-call-websocket` (`ciap-ingestion-service`) — including its live
transcription WebSocket.

- **Application name:** `transcript-processing-service`
- **Base package:** `com.snet.transcriptprocessing`
- **Framework:** Spring Boot `3.4.1`, Java `21`
- **Messaging:** Apache Kafka (`spring-kafka`)
- **Persistence:** PostgreSQL via Spring Data JPA / Hibernate
- **Downstream:** `ciap-api` REST (`ciap.api.base-url`)

---

## Tech stack

| Concern     | Choice                                             |
| ----------- | -------------------------------------------------- |
| Framework   | Spring Boot (`spring-boot-starter-web`)            |
| Realtime    | WebSocket ingestion (`spring-boot-starter-websocket`) |
| Messaging   | Apache Kafka — consumer & producer (`spring-kafka`)|
| Persistence | Spring Data JPA + Hibernate (`spring-boot-starter-data-jpa`) |
| Database    | PostgreSQL (`org.postgresql:postgresql`)           |
| Downstream  | `RestTemplate` client to `ciap-api`                |
| Tests       | `spring-boot-starter-test`, `spring-kafka-test`    |
| Build       | Maven (`pom.xml`)                                  |

---

## Folder structure

```
ciap-kafka/
├── pom.xml                     # Maven build descriptor (Spring Boot 3.4.1, Java 21)
├── README.md
└── src/
    ├── main/
    │   ├── java/com/snet/transcriptprocessing/
    │   │   ├── TranscriptProcessingApplication.java   # @SpringBootApplication entry point
    │   │   ├── config/
    │   │   │   ├── AppConfig.java                       # ObjectMapper / RestTemplate beans
    │   │   │   ├── CorsConfig.java                      # CORS registration
    │   │   │   ├── KafkaConsumerConfig.java             # consumer factory / listener container
    │   │   │   └── KafkaProducerConfig.java             # producer factory / KafkaTemplate
    │   │   ├── consumer/
    │   │   │   └── EventKafkaConsumer.java              # @KafkaListener -> forwards to ciap-api
    │   │   ├── controller/
    │   │   │   ├── EventController.java                 # POST /api/events            [app.producer.enabled]
    │   │   │   ├── BicomWebhookController.java          # POST /api/webhooks/bicom/*  [app.producer.enabled]
    │   │   │   └── HealthController.java                # GET /health
    │   │   ├── ws/
    │   │   │   ├── WsIngestionConfig.java               # registers WS /transcript    [app.ws.enabled]
    │   │   │   └── TranscriptIngestHandler.java         # PBXware live transcription -> Kafka
    │   │   ├── dto/
    │   │   │   └── IncomingEventRequest.java            # { module, payload }
    │   │   ├── model/
    │   │   │   └── TelephonyEvent.java                  # @Entity (table telephony_event)
    │   │   ├── producer/
    │   │   │   └── EventKafkaProducer.java              # publishes to Kafka
    │   │   ├── repository/
    │   │   │   └── TelephonyEventRepository.java        # JpaRepository
    │   │   └── service/
    │   │       ├── EventIngestionService.java           # persist + publish orchestration
    │   │       └── CiapRestClient.java                  # RestTemplate -> ciap-api
    │   └── resources/
    │       ├── application.properties                   # shared config, activates `local` profile
    │       ├── application-local.properties             # local overrides (env-var defaults)
    │       ├── application-qa.properties                # QA profile
    │       └── application-prod.properties              # prod profile
    └── test/java/com/snet/transcriptprocessing/
        └── consumer/EventKafkaConsumerTest.java
```

---

## HTTP ingestion endpoint

- `POST /api/events` — body `IncomingEventRequest`:
  ```json
  { "module": "transcript" | "event", "payload": { /* arbitrary JSON */ } }
  ```
  Both `module` and `payload` are required (validated). On success returns
  `{ "status": "SUCCESS", "module": "...", "message": "Event ingested and published to Kafka" }`.

---

## Bicom PBXware webhook endpoints

Java replacement for the Node `ciap-call-websocket` receiver. Configure these URLs in
PBXware's Event Publisher. Base URL = `http://<ciap-kafka-host>:8081`.

| Purpose | Method | Path |
|---|---|---|
| **Call events** (primary) | `POST` | `/api/webhooks/bicom/events` |
| Call events (alias) | `POST` | `/api/webhooks/bicom/pbx/event` |
| **Transcripts** | `POST` | `/api/webhooks/bicom/transcript` |

- `…/events` and `…/pbx/event` are the same handler (mirrors the two Node paths) — configure one, use `/events`.
- **Body:** raw JSON, any `Content-Type` (accepted as raw text, like the Node `express.text({type:"*/*"})`).
- **Headers:**
  - `x-shared-secret` — required **only if** `app.webhook.shared-secret` (env `EVENT_MANAGER_SHARED_SECRET`) is set; when blank the check is skipped (Node default). Mismatch → `401 {"ok":false,"error":"unauthorized"}`.
  - `x-connection-id` (transcript only, optional) — used as the Kafka message key for per-connection correlation; a UUID is generated if omitted.
- **Response:** `200 {"ok":true}`.
- **Flow:** raw body → `{ "module": "event" | "transcript", "payload": <parsed JSON or raw string> }` → `EventKafkaProducer.publish(key, payload)` → topic `app.kafka.topic`. Events key = `"pbx-event"`; transcripts key = the connection id. Published **directly** to Kafka (no DB persist), unlike `/api/events`.

> **Deploy notes (carried from the Node app):**
> - The Node webhook listened on **:6565**; this service is on **:8081** — update the PBXware webhook URL.
> - Topic spelling: this service defaults to `telephony.events` (dot); the Node default was `telephony-events` (hyphen). Set `KAFKA_TOPIC` so the producer here and the consumer agree.

---

## WebSocket ingestion endpoint

Live-transcription **WebSocket server** — the Java replacement for the Node
`ciap-ingestion-service` `WS /transcript`. PBXware's Live Transcription connects to
it as a client and streams frames. Enabled by `app.ws.enabled` (default `true`).

| Purpose | Protocol | Path (default) |
|---|---|---|
| **Live transcription ingestion** | WebSocket | `ws://<ciap-kafka-host>:8081/transcript` |

- On connect the server replies `{"type":"connection_ack","status":"success","timestamp":…}` and assigns a per-connection **UUID** (used as the Kafka message key — the Node `connectionId`).
- Each inbound frame is inspected; **only a completed transcript** is forwarded to Kafka:
  `transcribed_data.type == "conversation.item.input_audio_transcription.completed"`. **Deltas are dropped** (logged at DEBUG) — matching the Node behaviour.
- Completed frames go through the same path as the HTTP `/transcript` webhook (`BicomWebhookService.publishTranscript`), so they land on Kafka as a `TelephonyEvent` the consumer understands.
- Idle sockets are kept alive with a periodic **ping** (`app.ws.ping-interval-ms`, default 25s).

**Config (only used when `app.ws.enabled=true`):**

| Key | Env | Default | Meaning |
|---|---|---|---|
| `app.ws.path` | `APP_WS_PATH` | `/transcript` | WebSocket path (matches the Node service) |
| `app.ws.allowed-origins` | `APP_WS_ALLOWED_ORIGINS` | `*` | Handshake origins (PBXware is a server client, not a browser) |
| `app.ws.ping-interval-ms` | `APP_WS_PING_INTERVAL_MS` | `25000` | Keepalive ping interval |

> This is a **separate socket** from ciap-api's `/ws/transcript` (:8080), which is the
> browser-facing realtime channel with per-user routing. Do not conflate the two —
> different host/port, path, and direction (PBXware→here vs here→browser).

> **Repointing PBXware:** the Node WS was on **:6565** `/transcript`; this one is on
> **:8081** `/transcript`. Update PBXware's Live Transcription URL (front with an nginx
> `location /transcript { … Upgrade/Connection headers … }` block if terminating TLS),
> then the Node ingestion service can be decommissioned.

---

## Deployment roles

The three ingestion/consumption concerns are each gated by `@ConditionalOnProperty`,
so **one jar** can be deployed as independent nodes (or all-in-one, the default).

| Flag | Env | Default | Enables |
|---|---|---|---|
| `app.ws.enabled` | `APP_WS_ENABLED` | `true` | WebSocket `/transcript` ingestion (PBXware → Kafka) |
| `app.producer.enabled` | `APP_PRODUCER_ENABLED` | `true` | HTTP endpoints `/api/events`, `/api/webhooks/bicom/*` → Kafka |
| `app.consumer.enabled` | `APP_CONSUMER_ENABLED` | `true` | Kafka `@KafkaListener` → forward to ciap-api |

Examples:

```bash
# WS-ingestion node (no HTTP producer, no consumer)
APP_PRODUCER_ENABLED=false APP_CONSUMER_ENABLED=false java -jar target/*.jar
# HTTP producer node
APP_WS_ENABLED=false APP_CONSUMER_ENABLED=false java -jar target/*.jar
# Consumer node
APP_WS_ENABLED=false APP_PRODUCER_ENABLED=false java -jar target/*.jar
# All-in-one (default) — no flags needed
java -jar target/*.jar
```

> Every role still needs the **DB** and the **Kafka broker**: the ingestion path
> persists to `telephony_event` *before* it publishes, and the consumer writes as it
> forwards. Point all nodes at the same PostgreSQL and Kafka.

---

## Message flow

```
WS  /transcript (PBXware live transcription) ─┐   [app.ws.enabled]
HTTP POST /api/webhooks/bicom/{events,transcript} ┤   [app.producer.enabled]
HTTP POST /api/events ────────────────────────────┴─► EventIngestionService
                           ├─► TelephonyEventRepository.save()   (PostgreSQL, table telephony_event)
                           └─► EventKafkaProducer.publish()      (Kafka topic: telephony.events)

Kafka topic telephony.events ─► EventKafkaConsumer (@KafkaListener)   [app.consumer.enabled]
                                   └─► CiapRestClient ─► ciap-api REST (ciap.api.base-url)
```
(The WS server forwards only *completed* transcripts; deltas are dropped.)

The consumer resolves the downstream endpoint by `module` (`transcript` vs
`event`); an unknown module is rejected with `IllegalArgumentException`.

---

## Prerequisites

- **JDK 21**
- **Maven 3.9+** (no wrapper committed — use a system `mvn`)
- **PostgreSQL** reachable (default DB `ciap`)
- **Apache Kafka** broker reachable (default `localhost:9092`)
- **`ciap-api`** running (default `http://localhost:8080`) for REST forwarding

---

## Configuration

Shared config is in [`application.properties`](src/main/resources/application.properties);
per-environment values live in `application-{local,qa,prod}.properties`, selected by
the active Spring profile (`spring.profiles.active`, default `local`).

**External config directory (same mechanism as ciap-api).** At startup the service
loads config from **one** directory in addition to the bundled classpath properties:

```
${config.dir | CONFIG_DIR | /d01/ciap/config}/     (+ classpath fallback)
```

Set it with `-Dconfig.dir=/path` or `CONFIG_DIR=/path`; the resolved path is logged
at boot (`Config location (single dir): …`). Drop environment overrides / secrets as
`application.properties` (or `application-<profile>.properties`) in that directory.

> Give each service its **own** directory. ciap-api and ciap-kafka both ship an
> `application.properties`, so pointing both `CONFIG_DIR`s at the same folder would let
> one read the other's config — e.g. `/d01/ciap/config` for api, `/d01/ciap/kafka/config`
> for kafka.

Shared defaults:

| Key                                       | Value                       |
| ----------------------------------------- | --------------------------- |
| `server.port`                             | `8081`                      |
| `spring.profiles.active`                  | `local`                     |
| `spring.kafka.consumer.group-id`          | `transcript-processing-group` |
| `spring.kafka.consumer.auto-offset-reset` | `earliest`                  |
| `app.kafka.topic`                         | `telephony.events`          |
| `spring.jpa.hibernate.ddl-auto`           | `update`                    |

The `local` profile reads these with `${ENV_VAR:default}` fallbacks, so they can be
overridden by environment variables:

| Env var                  | Default (local)                                     |
| ------------------------ | --------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`| `localhost:9092`                                    |
| `CIAP_API_BASE_URL`      | `http://localhost:8080`                             |
| `DB_URL`                 | `jdbc:postgresql://localhost:5432/ciap` |
| `DB_USERNAME`            | `postgres`                                          |
| `DB_PASSWORD`            | `<set locally / override in prod>`                  |

Select a profile at runtime, e.g.:

```bash
java -jar target/*.jar --spring.profiles.active=qa
# or: SPRING_PROFILES_ACTIVE=prod java -jar target/*.jar
```

> Provide DB credentials, the Kafka broker, and `ciap.api.base-url` via
> environment variables / a secrets manager in shared environments — do not
> commit real values.

---

## Build & run (local)

```bash
# 1. Start PostgreSQL (DB: ciap), Kafka (:9092), and ciap-api (:8080)

# 2. Build
mvn clean package

# 3a. Run the jar
java -jar target/transcript-processing-service-0.0.1-SNAPSHOT.jar

# 3b. …or with the Spring Boot plugin
mvn spring-boot:run
```

Service starts on `http://localhost:8081`, exposes `POST /api/events`, and begins
consuming from `telephony.events`.

Run tests:

```bash
mvn test
```

---

## Deployment

Self-contained Spring Boot fat jar (same model as `ciap-api`).

1. **Build:** `mvn clean package -DskipTests` → `target/transcript-processing-service-0.0.1-SNAPSHOT.jar`.
2. **Provision** a JDK 21 host with network access to PostgreSQL, the Kafka broker,
   and `ciap-api`.
3. **Run** the jar (e.g. via `systemd`, mirroring the `ciap-api` unit) with the
   profile and connection settings supplied as environment variables:
   ```bash
   SPRING_PROFILES_ACTIVE=prod \
   KAFKA_BOOTSTRAP_SERVERS=<broker-host>:9092 \
   CIAP_API_BASE_URL=https://<ciap-api-host> \
   DB_URL=jdbc:postgresql://<db-host>:5432/ciap \
   DB_USERNAME=<user> DB_PASSWORD=<password> \
   java -jar transcript-processing-service.jar
   ```

---

## Platform context

`ciap-kafka` is one of four CIP repositories:

- **`ciap-web`** — React micro-frontend.
- **`ciap-api`** — REST + auth + WebSocket + PostgreSQL (this service forwards to it).
- **`ciap-kafka`** — this service (HTTP ingestion + Kafka + JPA/PostgreSQL).
- **`ciap-call-websocket`** — legacy Node ingestion service (`ciap-ingestion-service`,
  :6565) that turned PBXware webhooks + live transcription into Kafka / REST traffic.
  **Being retired** — this service now covers its webhook **and** WebSocket
  `/transcript` ingestion. Its `/extensions` list is already in ciap-api
  (`/api/directory/extensions`); nothing in CIP consumes its `/dids`, so no port needed
  unless an external team polls it.
#   c a  
 
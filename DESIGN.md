# Trail Condition ETL Pipeline: Critical Design Review

This repository implements a **distributed, resilient ETL pipeline** designed to ingest external weather data from `Open Weather Maps`, apply a rule-based classification, and update an internal data store. The system uses a message-passing architecture leveraging **Redis Streams** for scalability and fault isolation.

---

## System Goals and Requirements Mapping

The system is engineered to meet key functional goals and non-functional requirements (NFRs) for reliability and external service consumption.

| Requirement Type                 | Requirement / Goal                                                       | Fulfillment Stage(s)             | Design Mechanism in Code                                                                                                                                                                                                                |
| :------------------------------- | :----------------------------------------------------------------------- | :------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Functional**                   | Populate internal weather databases.                                     | `fetch-weather`                  | Posts transformed Historical and Forecast data to the internal API endpoint.                                                                                                                                                            |
| **Functional**                   | Label trail conditions.                                                  | `categorize-trail-conditions`    | Applies a classification algorithm to generate trail conditions and posts the resulting labels to the internal API.                                                                                                                     |
| **NFR: Scalability**             | Scale to a large number of locations (Horizontal Scaling).               | All Services                     | Redis Streams Consumer Groups distribute task load across multiple parallel worker instances.                                                                                                                                           |
| **NFR: Third-Party Consumption** | Apply Rate Limits.                                                       | `fetch-weather`                  | `outpost-etl-core` provides mechanisims to enforce maximum limits and control request rates to third party API. This service uses those.                                                                                                |
| **NFR: Resilience**              | Tolerate persistent external failures.                                   | `fetch-weather`                  | Circuit breakers protect workers from being blocked by slow/unresponsive services, and secondarily protect downstream APIs by reducing inbound load during failures. Retry mechanisms and dead letter queues provide additional safety. |
| **NFR: Data Integrity**          | High probability of at-least-once processing end-to-end within 24 hours. | `schedule-jobs`, `fetch-weather` | Transactional Outbox Pattern ensures task durability once it is created.                                                                                                                                                                |
| **NFR: Reliability**             | Recover from failed worker crashes.                                      | All Services                     | Redis Streams Pending Entry List (PEL) tracks unacknowledged messages so workers can explicitly reclaim and retry them. Transactional outbox persists tasks across restarts.                                                            |

---

## Architecture and Design Analysis

The pipeline consists of three decoupled services connected by asynchronous Redis Streams queues.

| Service Name                    | Role                       | Pipeline Functionality                                                            | Upstream Queue              | Downstream Queue            |
| :------------------------------ | :------------------------- | :-------------------------------------------------------------------------------- | :-------------------------- | :-------------------------- |
| **schedule-jobs**               | Orchestration              | Fetches location data and generates ingestion tasks.                              | None                        | `owm-ingestion-stream`      |
| **fetch-weather**               | Ingestion & Transformation | Fetches, transforms, and loads raw weather data to the internal API.              | `owm-ingestion-stream`      | `owm-categorization-stream` |
| **categorize-trail-conditions** | Processing & Loading       | Retrieves internal data, applies business logic, and posts final classifications. | `owm-categorization-stream` | None                        |

### Strengths: Resilience and Integrity

- **Transactional Integrity:** The use of the transactional outbox pattern in the first two stages is a core mechanism for Outbox ensures no pre-publish task loss. Once published, Redis Streams provides at-least-once semantics.
- **Fault Tolerance:** Reliance on circuit breakers and retries with exponential backoff and jitter ensures transient network failures are managed and persistent service failures do not lead to cascading failures. Dead letter queues provide a redrive mechanism.
- **Concurrency:** The trasactional outbox pattern is handled using multi-threaded workers to maximize throughput. One thread writes to disk and one flushes the disk to a downstream queue. When an upstream queue exists, a separate thread reads from it. Some read/write contention may occur with the Sqlite database, but networking requests are independent I/O.
- **Deduplication:** A centralized `deduplication` cache is used to reduce the likelihood of re-processing messages that generate costly API requests and/or ML model evaluations.
- **Dead Letter Queue:** Provides visibility into system failures, provides a simple mechanisms to raise alerts on message failure, and allows redriving of failed messages as needed.
- **Clock drift** is handled by calculating timestamps relative to Redis server time to minimize the probability of improper labeling. TODO: Apply this to daily rate limits.
- **Idempotency:** Because Redis streams provides at-least-once delivery, tasks transformations and writes should be idempotent. Data is upserted into the database, making multiple writes safe.

### Trade-offs and Shortcomings

- **High Availability (HA):** The system relies on a single Redis master instance for the message broker.
- **Worker Complexity:** The multi-threaded architecture of the Ingestion worker is complex to monitor and debug, increasing the risk of subtle concurrency bugs and deadlocks. The trade-off is accepted for the benefit of high concurrency when dealing with many parallel network calls.
- **Business Logic Agility:** The **Rule-Based Classifier** is brittle and non-adaptive. This is mitigated through a contract for classification: i.e., we can easily swap in an ML model later when we have the data.
- **Dependency Coupling:** Workers are tightly coupled to the internal Weather Service API and external Weather API. The internal API should practice versioned releases for safe updates. The system should be monitored for external API failures / updates, and a migration plan should be established for a different API.
- **Filesystem throughput:** Transactional outbox pattern provides durability at the cost of high disk IO. Multithreading helps here, as it releases the Global Interpreter Lock, but this can become a bottleneck, especially with Sqlite and file locks.
- **Redis implementation:** Currently, the Redis DAO objects do not use Lua scripts to enforce atomicity of key commands. Additionally, Rate limiter classes need to use the centralized server clock to ensure timing issues don't cause overcharged accounts. Care might need to be taken if Redis replicas are used at some point.
- **Scheduler deduplication**: Currently, `etl` only supports `UUID` task IDs and associated deduplication. Ideally, task IDs would be the `date` or `date+hour` that the data is being fetched for.
- **Security:** Currently, the third party service is mocked, so there is no API key. Internal services are currently unprotected for local development. This needs to be actively developed.
- **Monitoring:** Currently, `RedisInsights` provides visibility into queues, `Adminer` into downstream databases, and logging is done per-container. But, additional monitoring and alerting is necessary.
- **Testing:** Some unit tests exist, but more robust tests need to be implemented. Currently, `outpost-dev` provides end-to-end simulation that can be monitored.

## Future work

The underlying `etl` package (`outpost-etl/core`) and the weather service (`outpost-weather-service`) are still under development.
Future changes in those packages may affect delivery guarantees and subsequent implementation details in this package.

Once the components are fully implemented, then it will be time to configure the system properly, including:

- Configuring rate limiters based on API documentation
- Configuring retry behavior
- Configuring circuit breaker error handling and dynamics

In addition, many of the Shortcomings listed in prior sections need to be addressed (e.g., security)

## A better design

For this particular pipeline, we could have two scheduler services that monitor the database for stale weather data and stale classifications.
These events could be triggered independently, removing the need for the intermediate worker, simplifying the worker code, and decoupling the two consumer stages.
The existing pipeline served as an experiment to test sequential ETL pipelines using Redis streams.

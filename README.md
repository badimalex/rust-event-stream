# rust-event-stream

`rust-event-stream` is a high-throughput service for ingesting and processing events over HTTP.

Clients send events through an HTTP API. The service validates incoming data, passes accepted events through a bounded asynchronous pipeline, and processes them in a background worker.

Accepted events are persisted to PostgreSQL using batched writes.

## Features

- HTTP event ingestion through `POST /v1/events`
- Input validation
- Asynchronous event processing
- Bounded Tokio `mpsc` pipeline
- Backpressure when the internal queue is full
- HTTP request body size limit
- Request timeout
- In-flight request concurrency limit
- Load shedding during overload
- Graceful shutdown with queue draining
- `/health` and `/ready` endpoints

## Performance

Benchmark: 100 concurrent clients, 30 seconds, 0% errors.

| Persistence strategy                                                                                         | Successful events |      Throughput |
| ------------------------------------------------------------------------------------------------------------ | ----------------: | --------------: |
| [Single-row writes baseline](https://github.com/badimalex/rust-event-stream/tree/perf-baseline-pre-batching) | 60,095 | 2,000 events/s |
| [Batched PostgreSQL writes](https://github.com/badimalex/rust-event-stream/commit/52dd1b90686ee768b58961afaf06244a0cff55f1) | 307,022 | 10,217 events/s |
**Result: ~5.1× higher error-free event persistence throughput.**


## Architecture

```text
Client
  ↓
Axum Router
  ↓
HTTP middleware
(body limit / timeout / concurrency limit / load shedding)
  ↓
Handler
  ↓
EventProducer
  ↓
Bounded mpsc queue
  ↓
Worker
  ↓
PostgresStorage
```

The bounded queue prevents unlimited memory growth when producers submit events faster than the worker can process them. When the queue is full, backpressure forces producers to wait instead of continuously accumulating work.

## HTTP API

### `POST /v1/events`

Accepts an event, validates it, and submits it to the asynchronous event pipeline.

Event fields:

```text
event_id
tenant_id
event_type
timestamp
payload
```

Successful requests return:

```text
201 Created
```


```bash
curl -i -X POST http://127.0.0.1:3000/v1/events \
  -H "Content-Type: application/json" \
  -d '{"event_id":"db-down-1","tenant_id":"t-1","event_type":"test","timestamp":123456789,"payload":"hello"}'

```


### `GET /health`

Basic liveness endpoint.

Successful requests return:

```text
200 OK
```

### `GET /ready`

Basic readiness endpoint.

At the current stage it returns `200 OK`. More complete readiness checks for the pipeline and external dependencies will be added as the project evolves.

## Overload Protection

- **Body limit** — limits the maximum HTTP request body size. Requests exceeding the limit receive `413 Payload Too Large`.
- **Timeout** — limits the maximum execution time of an accepted request. Requests exceeding the deadline receive `408 Request Timeout`.
- **Concurrency limit** — limits the maximum number of HTTP requests executing simultaneously. The current limit is `100` in-flight requests.
- **Load shedding** — rejects excess work when the concurrency limit is exhausted instead of making additional requests wait. Rejected requests receive `503 Service Unavailable`.

The HTTP overload layer works together with the bounded internal pipeline: HTTP concurrency is controlled independently from event queue capacity.

## Reliability

During graceful shutdown, the service stops accepting new work.

Events that were already accepted are not discarded. The worker drains the remaining queue and processes accepted events before terminating.

The application completes shutdown after the worker has finished processing the remaining work.

## Running Locally

### Requirements

- Rust toolchain
- PostgreSQL
- `sqlx-cli`

### Run

```bash
sqlx migrate run
```

```bash
cargo run
```

The server starts on:

```text
http://127.0.0.1:3000
```

Check the health endpoint:

```bash
curl http://127.0.0.1:3000/health
```

Example event request:

```bash
curl -i \
  -X POST http://127.0.0.1:3000/v1/events \
  -H 'content-type: application/json' \
  -d '{
    "event_id": "event-1",
    "tenant_id": "tenant-1",
    "event_type": "click",
    "timestamp": 1700000000,
    "payload": "example"
  }'
```

## Testing

```bash
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```

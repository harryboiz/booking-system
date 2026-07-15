# Ticket Event API

## Run the full project locally

### Prerequisites

- Go 1.23 or later
- Docker with Docker Compose

All commands below must be run from the repository root.

### 1. Start the infrastructure

Start PostgreSQL, Redis, and Kafka, then create the 100-partition `ticket` topic:

```bash
docker compose up -d postgres redis kafka
docker compose run --rm kafka-init
```

The local ports are PostgreSQL `5432`, Redis `6379`, and Kafka `9092`.

### 2. Apply the database migrations

```bash
go run github.com/pressly/goose/v3/cmd/goose@v3.24.3 \
  -dir migrations \
  postgres 'postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable' up
```

### 3. Start all application processes

Run each command in a separate terminal from the repository root.

Terminal 1 — HTTP API:

```bash
go run ./cmd/api
```

Terminal 2 — ticket update consumer:

```bash
go run ./cmd/update_ticket_consumer
```

Terminal 3 — expired-ticket cancellation job:

```bash
go run ./cmd/cancellation_job
```

The services use the local YAML files under `config/` by default. Environment
variables from `.env.example` can be exported to override those defaults.

### 4. Verify the API

```bash
curl -i http://localhost:8080/health
```

The expected response is `200 OK`.

### 5. Stop the project

Stop the three Go processes with `Ctrl+C`, then stop the infrastructure:

```bash
docker compose down
```

To remove local PostgreSQL, Redis, and Kafka data as well, run:

```bash
docker compose down -v
```

## Documentation

See the [`docs/`](./docs/) directory:

- [High-level design](./docs/high-level-design.md)
- [API service](./docs/api-service.md)
- [Ticket update consumer](./docs/ticket-update-consumer.md)
- [Cancellation job](./docs/cancellation-job.md)
- [PayPal simulator](./docs/paypal-simulator.md)

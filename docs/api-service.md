# API Service

## Responsibility

The API service exposes the synchronous HTTP boundary for event management and
ticket workflows. It listens on `http://localhost:8080` by default and is started
with:

```bash
go run ./cmd/api
```

## Dependencies

- PostgreSQL for event CRUD and completed-ticket reads.
- Redis for event, pending-ticket, client-order, and user-ticket snapshots.
- Kafka for `pending` and `confirm` commands.
- An in-process PayPal simulator for order creation and capture.

The service verifies PostgreSQL and Redis connectivity during startup. It does not
run database migrations automatically.

## HTTP endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness response |
| `POST` | `/events` | Create an event |
| `GET` | `/events` | List events |
| `GET` | `/events/{id}` | Get an event |
| `PUT` | `/events/{id}` | Replace editable event fields |
| `DELETE` | `/events/{id}` | Delete an event |
| `GET` | `/tickets` | Find a ticket by ticket ID or client order ID |
| `POST` | `/tickets/pending` | Request a pending reservation |
| `POST` | `/tickets/payment` | Create a simulated PayPal order |
| `POST` | `/tickets/confirm` | Capture payment and request confirmation |

## Event API

Create and update requests use this shape:

```json
{
  "name": "Go Conference",
  "description": "A conference for Go developers",
  "start_date": "2026-09-10T09:00:00+07:00",
  "end_time": "2026-09-10T18:00:00+07:00",
  "total_tickets": 200,
  "max_ticket_per_user": 4,
  "ticket_price": 49.5
}
```

Dates use RFC 3339. `end_time` cannot precede `start_date`, and
`max_ticket_per_user` must be greater than zero. Ticket statistics in event
responses are maintained by the ticket update consumer.

Event mutations write PostgreSQL directly. The API does not update the event
snapshot in Redis; the consumer's startup reconciliation or a later processed
ticket batch refreshes it. For a new local database, create events before starting
the consumer, or restart the consumer after creating them, before requesting
pending tickets.

## Pending-ticket request

```http
POST /tickets/pending
Content-Type: application/json

{
  "user_id": 10,
  "event_id": 1,
  "client_order_id": "order-20260715-0001"
}
```

The API first checks
`tickets:client-order-id:{user_id}:{client_order_id}`. If it already maps to a
ticket UUID, the same UUID is returned with `202 Accepted` and no message is
published again.

For a new request, the API uses Redis event and per-user snapshots for an early
availability check, creates a UUID, publishes a `pending` command, and stores the
client-order mapping. The consumer performs the durable transition asynchronously.
Possible business conflicts include `tickets sold out` and
`user ticket limit reached`.

## Ticket lookup

`GET /tickets` requires `user_id` and exactly one of `ticket_id` or
`client_order_id`:

```bash
curl 'http://localhost:8080/tickets?user_id=10&ticket_id=c7bca801-a080-45c9-972c-860cd4e44ab6'
```

Pending tickets are read from Redis. If no pending snapshot exists, the API reads
the latest matching terminal record from `ticket_done` in PostgreSQL. Ownership is
part of the lookup, so a ticket belonging to another user is returned as not found.

## Payment and confirmation

`POST /tickets/payment` accepts a `user_id` and pending `ticket_id`. The API loads
the ticket and event snapshots, then creates a PayPal-shaped `CAPTURE` order in the
in-process simulator. The ticket ID is the idempotency key and the event ticket
price is expressed in USD.

`POST /tickets/confirm` validates that the pending ticket belongs to the user,
captures its simulated payment order, and publishes a `confirm` message. The
response is `202 Accepted`; the ticket becomes `confirm` only after the consumer
commits the transition.

Because simulator state is in memory, payment creation and confirmation must reach
the same running API process. Restarting the API removes all simulated orders.

## Configuration

The base configuration is `config/services/api/config.local.yml`, which includes
the shared PostgreSQL, Redis, and Kafka YAML files. Supported overrides are:

- `DATABASE_URL`
- `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB`
- `KAFKA_BROKERS` as a comma-separated list
- `KAFKA_TOPIC`

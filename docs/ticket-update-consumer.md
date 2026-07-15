# Ticket Update Consumer

## Responsibility

The ticket update consumer is the authoritative processor for asynchronous ticket
state changes. It consumes `pending`, `confirm`, and `cancel` commands, persists a
batch transactionally, refreshes Redis projections, and then commits Kafka offsets.

Start the first local consumer, which owns message keys `0` through `49`, with:

```bash
go run ./cmd/update_ticket_consumer
```

Start the second local consumer, which owns message keys `50` through `99`, in
another terminal:

```bash
WORKER_MESSAGE_KEYS="50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99" go run ./cmd/update_ticket_consumer
```

## Startup reconciliation

Before consuming Kafka, the process queries PostgreSQL for events whose
`event_id % 100` belongs to its configured `message_keys` and whose end time is
more than one day in the future. It rebuilds the corresponding event, pending
ticket, completed-ticket lookup, and per-user counter snapshots in Redis.

PostgreSQL remains the source of truth. Redis update failures are logged and can be
repaired by a later batch or process restart.

## Message contract

Messages on the `ticket` topic have this JSON shape:

```json
{
  "id": "c7bca801-a080-45c9-972c-860cd4e44ab6",
  "user_id": 10,
  "event_id": 1,
  "client_order_id": "order-20260715-0001",
  "status": "pending"
}
```

The key and partition must both equal `event_id % 100`. Malformed messages,
mismatched shards, invalid IDs, empty client order IDs, and unknown statuses are
logged and skipped.

## State transitions

| Command | Preconditions | Database effect |
| --- | --- | --- |
| `pending` | Ticket ID is new, event capacity remains, and the user is below the event limit | Insert into `tickets`; increment event pending count and `user_ticket.ticket_count` |
| `confirm` | Ticket exists in `tickets` as pending and is not already terminal | Move it to `ticket_done` with `confirm`; decrement pending count and increment confirm count |
| `cancel` | Ticket is pending, belongs to the event, and is older than `cancel_after` | Move it to `ticket_done` with `cancelled`; decrement pending and user counts, increment cancel count |

Ticket changes, event counters, and per-user counters are persisted in one
PostgreSQL transaction. Duplicate ticket IDs and transitions that are already
terminal or invalid for the current state are ignored.

## Batching and delivery semantics

The consumer opens one explicit-partition Kafka reader per configured message key.
It collects up to `batch_size` records or waits up to `batch_wait`. If processing
fails, the same batch is retried every second. Kafka offsets are committed only
after processing succeeds; offset commits are also retried.

This is an at-least-once pipeline. Processor-level state checks make replayed
ticket commands idempotent. All messages for an event share one partition so their
order is preserved within that event shard.

## Local shard ownership

The default local configuration assigns message keys `0` through `49` to the first
consumer. The second local consumer uses `WORKER_MESSAGE_KEYS` to own keys `50`
through `99`, as shown above. Together they cover every event shard without
overlapping ownership.

To cover every event with one local consumer instead, set:

```bash
export WORKER_MESSAGE_KEYS="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99"
go run ./cmd/update_ticket_consumer
```

Do not assign the same message key to multiple live consumer processes.

## Configuration

The base configuration is `config/services/worker/config.local.yml`. Supported
overrides are:

- `DATABASE_URL`
- `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB`
- `KAFKA_BROKERS`, `KAFKA_TOPIC`
- `WORKER_GROUP_ID`
- `WORKER_MESSAGE_KEYS`
- `WORKER_BATCH_SIZE` from 1 to 10,000
- `WORKER_BATCH_WAIT` as a positive Go duration
- `WORKER_CANCEL_AFTER` as a positive Go duration

# Cancellation Job

## Responsibility

The cancellation job finds pending reservations that exceeded their confirmation
window, attempts any required payment refund through its PayPal adapter, and
publishes `cancel` commands to Kafka. It never updates ticket status directly.

Start it with:

```bash
go run ./cmd/cancellation_job
```

## Polling flow

The job runs one poll immediately at startup and repeats every `poll_interval`:

1. Calculate `now - cancel_after`.
2. Query up to `batch_size` rows from `tickets` whose status is `pending` and whose
   `created_at` is at or before the cutoff.
3. Ask the payment adapter to refund a completed capture, if one exists.
4. Publish a `cancel` message for each ticket whose refund step succeeded or did
   not require a refund.
5. Leave the ticket unchanged in PostgreSQL until the ticket update consumer
   validates and persists the cancellation.

Errors are isolated per ticket. A refund or publish failure prevents cancellation
for that ticket during the current poll but does not prevent other tickets from
being processed. The failed ticket remains pending and is retried later.

## Idempotency

Repeated `cancel` messages are safe because the ticket update consumer ignores a
ticket that is already terminal. The refund request uses a value derived from the
ticket UUID as its idempotency key, so a shared durable PayPal implementation could
return the same refund for a retry.

## PayPal simulator limitation

The current job creates its own in-process PayPal simulator. That simulator is not
the same instance used by the API process, so it cannot observe orders or captures
created through the API. In the current local architecture, its refund check
therefore reports that no refund is required and the job proceeds to publish the
cancel command.

This is acceptable only as a development stub. A real integration should inject a
shared external PayPal client or another durable payment provider so the API and
cancellation job observe the same payment state.

## Configuration

The base configuration is
`config/services/cancellation/config.local.yml`. Its local defaults are a batch of
10,000 tickets, a one-minute poll interval, and a 20-minute expiration window.
Supported overrides are:

- `DATABASE_URL`
- `KAFKA_BROKERS`, `KAFKA_TOPIC`
- `CANCELLATION_BATCH_SIZE` from 1 to 10,000
- `CANCELLATION_POLL_INTERVAL` as a positive Go duration
- `CANCELLATION_CANCEL_AFTER` as a positive Go duration

The worker's `cancel_after` should match this job's expiration window. The worker
rechecks ticket age before applying a `cancel` command.

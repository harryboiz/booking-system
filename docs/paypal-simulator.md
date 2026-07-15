# PayPal Simulator

## Purpose

The package in `shared/paypal` is a concurrency-safe, in-memory test double shaped
after the PayPal Orders v2 and Payments v2 APIs. It lets the ticket workflow create
an order, capture it, and refund a capture without network calls or PayPal
credentials.

It is a Go library embedded by other processes, not an HTTP server and not a
standalone service.

## Supported operations

| Operation | Simulated PayPal endpoint | Initial result | Idempotent replay |
| --- | --- | --- | --- |
| Create order | `POST /v2/checkout/orders` | `201 Created` | `200 OK` with the original order |
| Capture order | `POST /v2/checkout/orders/{id}/capture` | `201 Created` | `200 OK` with the original capture |
| Refund capture | `POST /v2/payments/captures/{id}/refund` | `201 Created` | `200 OK` with the original refund |

The simulator returns PayPal-shaped resource IDs, statuses, purchase units,
captures, refunds, and HATEOAS links. Generated URLs use PayPal sandbox hostnames
for realistic response shapes, but no request is sent to those hosts.

## Ticket integration

The API creates an order with:

- intent `CAPTURE`;
- one purchase unit;
- the ticket UUID as `reference_id` and `PayPal-Request-Id`;
- the user ID as `custom_id`;
- the client order ID as `invoice_id`;
- the event ticket price in `USD`.

Confirmation finds the order by ticket UUID, verifies the user, and captures it
with `capture-{ticket_id}` as the idempotency key. The cancellation adapter searches
for the first completed capture and requests a full refund using
`refund-{ticket_id}`.

## Validation and behavior

- Reusing a create-order idempotency key with a different body returns an
  idempotency conflict.
- Capturing an unknown order returns an order-not-found error.
- A capture cannot be repeated with a different capture idempotency key.
- The ticket adapter rejects a user who does not own the simulated payment.
- Omitting a refund amount refunds the remaining captured amount in full.
- Re-refunding an already fully refunded capture returns an error unless the exact
  idempotent request is replayed.
- A `CREATED` order is considered expired after three hours by the ticket API.

## Storage and process boundaries

All state is protected by a mutex and stored in Go maps. It disappears when the
owning process stops. Separate calls to `paypal.NewSimulator()` create completely
independent stores.

The API constructs one simulator instance and shares it across its ticket handlers,
so create and capture operations work while that API process remains alive. The
cancellation job constructs a different instance in another process, so it cannot
see the API's orders or captures. The simulator is therefore suitable for local
behavior and unit tests, but it does not model shared, durable payment state.

## Testing

The simulator accepts a context on every operation, supports deterministic
idempotency keys, and has unit coverage in `shared/paypal/simulator_test.go`. Tests
can instantiate a fresh simulator to isolate payment state without any external
dependency.

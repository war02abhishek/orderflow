# The checkout saga, the Outbox pattern, and this system's consistency model

## The saga, step by step

1. `orders` receives `POST /checkout`, calls `inventory` **synchronously**
   to reserve stock. This is a plain precondition, not a saga step — a real
   checkout needs to know immediately whether the item exists.
2. On a successful reservation, `orders` writes the `Order` row
   (`status=AWAITING_PAYMENT`) **and** an outbox row for `PaymentRequested`
   in the same local transaction. On a failed reservation, the order goes
   straight to `REJECTED` — no saga, nothing published.
3. A scheduled relay (`OutboxRelay`, polling every 500ms) picks up the
   unpublished row and publishes it to Kafka.
4. `payment` consumes `PaymentRequested`, simulates processing (a
   configurable failure rate), and publishes `PaymentCompleted` or
   `PaymentFailed`.
5. `orders`, acting as the saga orchestrator, consumes the result:
   - **Success** → `Order` moves to `CONFIRMED`, an `OrderPlaced` outbox row
     is written.
   - **Failure** → the compensating transaction: `orders` calls
     `inventory`'s `/release` endpoint synchronously to give back the
     reserved stock, `Order` moves to `CANCELLED`, an `OrderCancelled`
     outbox row is written.
6. `notifications` consumes `OrderPlaced`/`OrderCancelled` and logs a
   simulated confirmation or cancellation notice.

Every state transition above and its outbox row commit together, in one
local transaction, in whichever service owns that transition. Nothing ever
publishes to Kafka directly from request-handling code.

## Why orchestration, not choreography

Two ways to build a saga: **choreography** (each service publishes an event
and reacts to others' events, with no central coordinator) or
**orchestration** (one service holds the saga's state machine and tells the
others what to do next). This system uses orchestration, with `orders` as
the orchestrator, for one concrete reason: **the compensating action is
`orders`' own call to make.** `orders` is the only service that knows both
"a reservation was made" and "payment failed" — it's the natural place for
the decision "release the reservation" to live. Choreography would mean
`inventory` reacting to a `PaymentFailed` event it has no other reason to
care about, just to know when to release stock it doesn't track ownership
of. Orchestration keeps that decision where the context already is.

The trade-off, honestly stated: orchestration makes `orders` a more complex
service (it now holds saga state, not just its own resource), and it's a
single point of coordination — if `orders` is unavailable, no saga can
progress. Choreography spreads that risk across services but makes the
overall flow harder to trace (there's no one place that says "here's what
happens after a checkout"). For a 3-step saga with one compensating action,
orchestration's simplicity wins.

## Why Outbox, not a direct publish

The alternative to the Outbox pattern is simpler-looking: after committing
the `Order` row, just call `kafkaTemplate.send(...)` directly. This is the
**dual-write problem**, and it's broken in a specific, easy-to-miss way —
there is no way to make "commit to Postgres" and "publish to Kafka" atomic
across two different systems. Either order of operations has a failure
window:

- **Commit, then publish**: a crash between the two means the DB says the
  order was reserved, but the payment service never gets asked to charge
  anything. The order sits in `AWAITING_PAYMENT` forever.
- **Publish, then commit**: a crash between the two means `payment` gets
  asked to charge an order that, as far as the database is concerned, was
  never created.

The Outbox pattern closes that window by making the "intent to publish" part
of the same transaction as the state change: the outbox row and the order
row either both commit or neither does, because they're one transaction
against one database. The separate relay process introduces its own gap
(between "committed" and "actually sent to Kafka"), but that gap is a
**duration**, not a **correctness** problem — the row is durably recorded
as unpublished, and the relay will find it on its next poll no matter how
long it takes. Nothing is ever lost, and nothing is ever fabricated relative
to what the database actually says happened.

## Consistency model, stated explicitly

**Strong consistency** exists only inside a single service's own
transaction: an order's state change and its outbox row commit together,
atomically, inside `orders`' database. Same for `inventory`'s stock
decrement (the atomic conditional update from Phase 1).

**Everything across services is eventually consistent.** Between "payment
requested" and "payment resolved," an order legitimately sits in
`AWAITING_PAYMENT` — that's not a bug, it's the saga doing its job. If
`payment` is unreachable, orders queue in that state rather than checkout
blocking or failing outright: this system chooses **availability over
immediate consistency** during a downstream partition, and recovers
correctness later via the saga's compensation path once `payment` (or the
network path to it) recovers.

This is also why the idempotency work in Phase 1 (G2) and Phase 2 (G17)
matters as much as the saga logic itself: eventual consistency means every
step might run more than once — a retried REST call, a redelivered Kafka
message after a rebalance — and the system's correctness depends on those
retries being safe, not on them never happening.

# Demo: order-fulfillment service

A complete microservice shape: provision a Domain + Feed, append
orders, and pump them through a processor with at-least-once
semantics, a dead-letter path, and graceful shutdown.

The contract in one line: **resolve = processed, reject = replay** —
the SDK acks a page only after every record in it has been handled.

```ts
import { ReaderStart, WhitewaterClient } from "whitewater-client";

const client = new WhitewaterClient({
  apiKey: process.env.WHITEWATER_API_KEY!,
  endpoints: ["http://localhost:7371", "http://localhost:7372"],
});

// ---- provision ------------------------------------------------------
await client.createDomain("shop");
const shop = await client.getDomain("shop");
await shop.createFeed("orders");
await shop.createFeed("orders.dlq");          // dead-letter feed

// ---- produce (usually a different service) ---------------------------
const writer = await shop.createWriter("orders").writerId("orders.ingest").open();
await writer.append("order-1", Buffer.from('{"total":42}'));

// ---- consume ----------------------------------------------------------
const deadLetter = await shop.createWriter("orders.dlq")
  .writerId("fulfillment.dlq").open();
const reader = await shop.createReader("orders")
  .readerId("fulfillment")                     // durable id — survives restarts
  .readerStartPoint(ReaderStart.BEGINNING)     // first-declare only
  .maxPageSize(250)                            // max records per fetch page
  .open();

// The event loop owns the pump — the promise is your failure surface.
const done = reader.forEach(async (m) => {
  try {
    await ordersDb.upsert(orderId(m), order(m));   // idempotent side effect
  } catch (e) {
    if (permanent(e)) {
      await deadLetter.append(m.key ?? "", m.value); // quarantine → continue → acked
      return;
    }
    throw e;                                       // transient → stop → replay
  }
});
done.catch((err) => {
  // A rejected handler stops the pump — restart or alert.
  console.error("reader pump died:", err);
  process.exitCode = 1;
});

// ---- shutdown ----------------------------------------------------------
// reader.close() ends the pump; the in-flight page stays unacked and
// replays on the next open.
process.on("SIGINT", () => void reader.close());
process.on("SIGTERM", () => void reader.close());
```

## Notes

- `readerId` is durable application state — the Kafka `groupId`
  analogue. `readerStartPoint` applies only on first declare; restarts
  resume the acknowledged frontier.
- Acks are per fetched page. A crash mid-page replays the whole page —
  that's what makes it at-least-once. Process idempotently.
- `maxPageSize(250)` bounds both the page and the replay window.
- `for await (const m of reader)` iterates with the same ack-per-page
  semantics; `break` leaves the tail unacked. Manual mode:
  `fetch()` → process → `ack()`. Never ack before processing — that's
  at-most-once.

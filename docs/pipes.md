# Pipes

A **Pipe** is a managed stream job: it drives one declared **Subscription**
and applies an operation to every record the Subscription delivers, writing
results to a Feed or a StateStore. Pipes run server-side — your process
does not need to stay alive for a Pipe to keep going.

| Operation | Reads from | Writes to | Purpose |
|---|---|---|---|
| `forward` | Subscription | Feed | copy records verbatim |
| `enrich` | Subscription + StateStore | Feed (+ optional quarantine Feed) | point-lookup enrichment |
| `window` | Subscription | StateStore (+ optional emit Feed) | fixed-window aggregation |

```ts
import { WhitewaterClient } from "whitewater";

const client = new WhitewaterClient({
  endpoints: ["localhost:7371"],
  apiKey: process.env.WHITEWATER_API_KEY ?? "",
});
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```ts
await client.createSubscription("orders.sub", "ops.orders");
// ReaderStart.beginning() is the default; ReaderStart.now()/cursor()/timestampNs()
// select other starts.
```

A Subscription can cover a whole Domain (typed-command form):

```ts
await client.commands([{
  command: "create_subscription",
  name: "ops.sub",
  domain: "ops",
  start: { kind: "beginning" },
}]);
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```ts
import { StateStore, Extractor } from "whitewater";

const spec = StateStore.named("ops.customers")
  .addKey("email", true, Extractor.json("$.email"))
  .addKey("tier", false, Extractor.json("$.tier"));
await client.stateStores.define(spec, "ops.customer.changes");
```

## 2. Forward pipes

```ts
await client.defineForwardPipe(
  "ops.orders.relay",   // pipe name (dotted)
  "orders.sub",         // source subscription
  "ops.orders.relayed", // output feed
);
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```ts
import { PipeOperation } from "whitewater";

await client.defineEnrichPipe(
  "ops.orders.enrich",
  "orders.sub",
  "ops.enriched",
  "ops.customers",                 // lookup store
  PipeOperation.enrich(Extractor.json("$.customerId"), {
    freshness: "latest",           // or "strict"
    named: "email",                // named key; omit for primary
    onMiss: "quarantine",          // "forward" | "drop" | "quarantine"
  }),
  "ops.enrich.misses",             // quarantine feed (required for "quarantine")
);
```

## 4. Window pipes

A window pipe's output store must be declared before the pipe — and it
must carry non-unique named keys `"key"` and `"pane"` so pane rows stay
enumerable without composite row keys:

```ts
await client.stateStores.define(
  StateStore.named("ops.order.counts").addKey("key").addKey("pane"),
);

await client.defineWindowPipe(
  "ops.orders.hourly",
  "orders.sub",
  "ops.order.counts",              // pane store (declared above)
  PipeOperation.window(
    Time.h(1),                     // 1h panes
    Extractor.json("$.region"),
    WindowAggregation.sum(Extractor.json("$.total")),
    {
      time: "event",               // or "ingest"
      watermark: Time.m(5),        // event-time close horizon
      grace: Time.m(1),            // revise emitted panes up to 1min late
      expire: Time.d(1),           // keep panes 24h
      keyLimit: 10_000n,
    },
  ),
  "ops.hourly.emits",              // optional emit feed for closed panes
);
```

Every window duration is a `Time` — `Time.h`, `Time.m`, `Time.s`,
`Time.ms`, `Time.us`, `Time.d`, `Time.ns`. The SDK extracts nanoseconds
internally for the wire; callers never spell raw nanosecond counts.

### Lateness models — pick exactly one

- `maxLate` — ingest-clock bound; later arrivals quarantine into a
  countable `late` row.
- `watermark` — event-time horizon `max(event_time) − watermark`
  closes a pane once it passes the pane end (refused under `"ingest"`);
  `grace` lets late events revise an emitted pane before counting into
  `late`.

An emit feed requires a declared lateness model.

### Aggregations

```ts
WindowAggregation.count()
WindowAggregation.sum(Extractor.json("$.total"))
WindowAggregation.average(Extractor.json("$.rtt"))
```

## 5. Lifecycle

```ts
const pipe = client.pipe("ops.orders.relay");

const info = await pipe.describe(); // stage, operation, outputs, status
await pipe.pause();   // cooperative; the frontier stays put
await pipe.resume();  // restarts from the durable frontier
await pipe.drop();

const all = await client.pipes();
```

`pause` is durable and idempotent — nothing is dropped while paused.
`describe()` returns `stage` (`declared`/`paused`), the opaque `operation`
node, and output Feed/Store IDs.

## 6. Watching demand

```ts
const demand = await client.demandPipes();
for (const row of demand.rows) console.log(row.name, row.stage);
```

## 7. Failure semantics

- Declarations commit atomically inside the command batch — create the
  subscription and the pipe in one `client.commands([...])` call to apply
  both or neither.
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.

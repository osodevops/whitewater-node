# whitewater-node

Node.js/TypeScript SDK for Whitewater Riverbeds. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Node 20+. No runtime dependencies — transport uses the global
`fetch`; bytes are `Buffer`; nanosecond times are `bigint` serialized as
decimal strings (never imprecise JS numbers).

## Clients

```ts
import { WhitewaterClient } from "whitewater-client";

const client = new WhitewaterClient({
  apiKey: process.env.WHITEWATER_API_KEY!,
  endpoints: [
    "https://node1:7070",
    "https://node2:7070",
    "https://node3:7070",
  ],
});
```

| Client | Covers |
|---|---|
| `client` (direct) | WCL + typed command execution (`/v1/admin/wcl`, `/v1/admin/commands`), node health — plus typed resource methods (`createDomain`, `createFeed`, `createWriter`, `createReader`, `createSubscription`, `describeFeed`, `renameFeed`, `dropFeed`, `listFeeds`, `seekReader`, `openWriterSession`, …) that build the same WCL for you. `ReaderStart` (`beginning()`, `now()`, `cursor(c)`, `timestampNs(ns)`) supplies `START AT` positions |
| `client.writer(name, epoch)` | Idempotent single and batch appends (`requestId` dedup) |
| `client.reader(name)` | Named-Reader sessions: open / fetch / ack / close / resume |
| `client.subscriptionMember(sub)` | Join, claim, renew, fetch, ack, release under fenced leases |
| `client.feeds` | Committed record reads, anonymous temporary fetches (`tail`/`waitMs`/`newOnly`), ad-hoc appends, `load(feed, start, sink)` one-shot history reads |
| `client.subscribe(reader, feed, start, onRecords)` | Managed durable consume: provisions the Reader, runs open → fetch → deliver → ack in the background; `await sub.close()` to stop |
| `client.stateStore(name)` / `client.stateStores` | Server-side StateStore facade — durable RF3 rows on the Nodes: `put`/`get`/`getBy` (named keys)/`iterate`/`delete`/`clear`/`drop`, plus `define` for declaration |
| `client.feed(name, codec?)` | Codec-bound typed Feed — `writer()`/`reader()` auto-declare and open sessions; `append`/`fetch`/`records`/`load`/`subscribe` serialize through the bound codecs |
| `client.store(name, codec?)` | Codec-bound typed StateStore — every `put`/`get`/`getBy`/`iterate` serializes through the bound codecs |

## Typed feeds and stores — codecs do the serialization

Bind a feed or store to codecs once; application code then works in its
own types — never manual `Buffer.from`/`JSON.parse` on every call:

```ts
import { codecs } from "whitewater-client";

interface Order { id: string; total: number }

// String keys + JSON payloads (one arg types the payload; two args
// bind key and payload codecs explicitly).
const orders = client.feed("orders.events", codecs.json<Order>());

const writer = await orders.writer("ingest");   // declares + opens session
await writer.append("order-1", { id: "order-1", total: 42 });
await writer.appendAll([{ requestId: rid, key: "o2", value: order2 }]);

const reader = await orders.reader("audit", 64); // declares + opens session
const page = await reader.fetch();
for (const m of page.records) console.log(m.key, m.value.total);
await reader.ack();
await reader.close();

const profiles = client.store("accounts.employees", codecs.json<Profile>());
await profiles.put("user-1", profile, { passport: "P111" });
const row = await profiles.get("user-1");        // row.value is Profile
for await (const r of profiles.iterate()) export(r);
```

```ts
codecs.utf8()          // UTF-8 text (JSON/XML/etc. as strings)
codecs.bytes()         // raw Buffer identity
codecs.json<T>()       // JSON.stringify/parse for any mapped type
codecs.int64()         // bigint as fixed-width 8-byte LE
codecs.custom(enc,dec) // protobuf, msgpack, application framing
textMetadata({k:"v"})  // text Metadata map -> wire Buffers
```

`client.feed(name)` binds the default `Feed<string, Buffer>`;
`feed.view(keyCodec, valueCodec)` and `store.view(...)` rebind codecs
without a wire call. The byte-level `client.feeds`, `client.writer`,
`client.reader`, and `client.stateStore` facades remain for raw access.

### Permanent consumers — the Kafka loop without the plumbing

`feed.consumer(name)` binds the durable named Reader (the
consumer-group equivalent) and owns the fetch → deliver → ack cycle:
transient failures retry with backoff, a fenced session re-opens,
progress resumes from the server-side cursor. Your code is a for-await
loop over decoded records:

```ts
for await (const msg of inFeed.consumer("enricher")) {
  await process(msg);              // msg.value is your type
}

// or a handler, resolving only on close():
await inFeed.consumer("enricher").onRecord(process).run();
```

- The page acks after every record's handler/yield completes — a
  thrown handler (or abandoned iteration) leaves it unacked, so it
  redelivers (at-least-once: dedupe effects via `requestId`, route
  poison pills to a DLQ yourself).
- `run()`/the iterator throw only for startup refusals (bad names,
  denied auth) — everything after the first open is supervised forever
  until `consumer.close()` (safe from signal handlers and from inside
  a handler; the in-flight page finishes and acks first).
- `.startAt(...)`, `.capacity(n)`, `.backoff(ms, ms)`, `.onError(fn)`,
  `.lastError`, `.isRunning`, `runAsync(handler)` tune and observe it;
  `feed.subscribe` remains for page-oriented control.

Value-only store lookups skip the row wrapper when freshness evidence
isn't needed — `null` on miss:

```ts
const user = await users.find(email);
const user2 = await users.findBy("user_id", userId);   // first match
const count = await counters.getOrDefault(bucket, 0);
```

A complete enrichment service — durable consumer, named-key fallback,
DLQ, idempotent outputs, counters — is
[examples/enrich-users.mjs](examples/enrich-users.mjs).

## Wire-level API

```ts
const spec = StateStore.named("accounts.employees")
  .addKey("passport", true).addKey("department", false);           // up to 10 named keys
const store = await client.stateStores.define(spec);               // durable store on the Nodes

await store.put("emp-1", payload, { passport: "P123", department: "eng" });
const row = await store.get("emp-1");              // StoreRow | null + freshness
const rows = await store.getBy("passport", "P123"); // named key — non-unique, a list
for await (const r of store.iterate()) export(r);
```

The store lives on the Nodes — the SDK holds no local copy. See
[docs/stores.md](docs/stores.md) for the full store guide.

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...). `Unavailable` covers transient refusals;
`AmbiguousResult` marks operations that may have committed — retry
through the same `requestId`, never blindly re-issue.

Cursors are branded opaque strings: never parsed, ordered, or
fabricated. `eventTimeNs`/`ingestTimeNs` are `bigint` — the full signed
64-bit nanosecond range round-trips exactly.

## Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free model
— names, never partitions or Nodes:

```ts
// Pipe declaration: the client names the business key, source and
// outputs — the server owns panes, leases and the effect journal.
await client.defineForwardPipe("orders.relay", "orders-sub",
  "orders.relayed");
await client.defineEnrichPipe("orders.enrich", "orders-sub",
  "orders.enriched", "crm.customers",
  PipeOperation.enrich(Extractor.recordKey(),
    { freshness: "latest", onMiss: "quarantine" }),
  "orders.enrich-dlq");
await client.defineWindowPipe("orders.hourly", "orders-sub",
  "stats.orders-hourly",
  PipeOperation.window(3_600_000_000_000,  // window_ns: 1h
    Extractor.recordKey(), WindowAggregation.count(),
    { time: "event",
      watermarkNs: 300_000_000_000,  // 5m horizon
      graceNs: 30_000_000_000,       // 30s late revisions
      expireNs: 7_200_000_000_000 }),
  "orders.hourly-emitted");

// Pipe lifecycle: describe, pause, resume, drop.
const pipe = client.pipe("orders.relay");
const info = await pipe.describe();    // stage, operation, outputs, status
await pipe.pause(); await pipe.resume(); await pipe.drop();
const pipes = await client.pipes();    // the declared catalog

// Index queries: exact, prefix, or range predicates over typed values.
const page = await client.queryIndex(
  "crm.byregion",
  indexPredicate.range(
    ["emea"], indexBound.inclusive(0), indexBound.exclusive(99)),
  { limit: 50, strict: true });
for (const row of page.rows) { /* row.key / row.payload Buffers */ }

// Demand observability: typed rows + per-domain rollups.
const report = await client.demandFeeds();
const appended = demandCounter(report.rows[0], "appended_records");
await client.demandReaders(); await client.demandSubscriptions();
await client.demandIndexes(); await client.demandPipes();
const hot = await client.hotKeys();    // dominant-key evidence per feed
```

Pipe operation payloads (`PipeInfo.operation`) and demand fields
(`DemandRow.fields`, `HotKey.evidence`) stay opaque objects so
server-side variants evolve without an SDK bump. `queryIndex` values
take `StoreValue` scalars — `bigint` narrows to i64, `number` must be a
safe integer.

## Build & test

```bash
npm install
npm run build       # tsc -> dist/ (JS + .d.ts)
npm test            # vitest: unit (+ conformance when env is set)
```

## Conformance

`test/conformance.test.ts` executes the shared fixture corpus against a
real three-Node Riverbed:

```powershell
$env:WHITEWATER_ENDPOINTS = "http://localhost:7371,http://localhost:7372,http://localhost:7373"
$env:WHITEWATER_SPEC_DIR  = "C:\Code\Stackapps\whitewater\sdk\spec\conformance"
npm run test:conformance
```

A scenario whose `op` the SDK cannot express fails explicitly rather
than silently skipping — a spec-required signal.

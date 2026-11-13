# whitewater-node

Node.js/TypeScript SDK for Whitewater. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Node 20+. No runtime dependencies — transport uses the global
`fetch`; bytes are `Buffer`; nanosecond times are `bigint` serialized as
decimal strings (never imprecise JS numbers).

A complete microservice walkthrough — provision, produce, pump-process,
dead-letter, shutdown — lives in
[`docs/demo-service.md`](docs/demo-service.md).

## Creating a client

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

`await client.healthy()` checks liveness. The client also carries the
flat administration surface — `wcl`/`commands`, node health, Domain and
Role lifecycle — while every Feed, Writer, Reader, Subscription,
Member, Pipe, StateStore, and Index is reached **only** through a
`Domain`.

Whitewater's public model is `domain -> feed -> key -> cursor ->
subscription`: partitions, ranges, replicas, and physical positions are
internal implementation details — cursors are opaque tokens, never
parsed.

## Domains — the strongly-typed front door

A Domain is the administrative namespace every business resource lives
inside: a Feed named `shop.orders` belongs to the Domain `shop`, and
nothing outside a Domain can reach it. Writers, Readers, Subscriptions,
StateStores, Indexes, and Pipes resolve their relative names inside the
owning Domain, so a `"fulfillment"` Reader declared on `shop` is
`shop.fulfillment` everywhere it matters: progress, leases, WCL, demand
reports, and access control all scope to it. Cross-Domain references are
possible but always explicit (`scoped.abs(...)` or a handle), which
makes accidental cross-wiring of two tenants' resources a deliberate
act rather than a silent mistake.

```ts
import { ReaderStart, WhitewaterClient } from "whitewater-client";

const shop = await client.createDomain("shop");  // CREATE DOMAIN shop;
// client.domain("shop") is the lazy form — zero I/O until needed
// client.getDomain("shop") verifies + materializes now
await shop.createFeed("orders");                 // CREATE FEED shop.orders;

await shop.feedNames();                          // ['shop.orders', 'shop.returns']
(await shop.feeds("order")).map((f) => f.name);  // prefix-filtered Feed handles
await shop.readers(); await shop.writers();      // scoped catalog views
await shop.subscriptions(); await shop.pipes();
await shop.indexes();       await shop.stateStores();
await shop.describe();      shop.historyPolicy;
await shop.refresh();                            // re-pull the cached snapshot

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
const writer = await shop
  .createWriter("orders")
  .writerId("ingest")                            // durable producer identity — required
  .open();

const reader = await shop
  .createReader("orders")
  .readerId("fulfillment")                       // durable consume identity — Kafka groupId
  .readerStartPoint(ReaderStart.BEGINNING)       // first-declare only — auto.offset.reset
  .maxPageSize(256)                              // max records per fetch
  .open();
```

`readerId` is durable application state: `readerStartPoint` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier. `writerId` is required because it's the Writer's
transactional identity: the fencing epoch binds to it and the
retry-dedup ledger lives under it. A `Feed` bound to another Domain is
refused locally, before any network call. The first catalog accessor
issues ONE batched control read (`describe domain` + every `show`
listing) and caches the snapshot — `refresh()` re-pulls.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```ts
await client.setDomainSchema("shop", {
  value: { format: "json", subject: "order-v1" },
});
await shop.setFeedSchema("orders", { key: { format: "text" } });

await shop.schema();                  // the Domain default
await shop.feedSchema("orders");      // effective: key=text, value=json/order-v1
await shop.declaredFeedSchema("orders");  // the Feed's own sides only
await shop.clearFeedSchema("orders"); // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`,
`text`, `bytes`, or `custom.<name>` — compatibility enforcement is
still a no-op.

## Working with Feeds

`feed("events", codecs.json<Order>())` binds a feed to key and payload
codecs once; every write encodes and every read decodes through them —
application code never calls `Buffer.from`/`JSON.parse` per record.
`domain.feed(relative)` binds the default `Feed<string, string>` — UTF-8
both ways, so plain JSON flows with no codec ceremony;
`feed.view(k, v)`/`store.view(...)` rebind codecs without a wire call.

```ts
import { codecs } from "whitewater-client";

interface Order { id: string; total: number }

const orders = ordersDomain.feed("events", codecs.json<Order>());

codecs.utf8()          // UTF-8 text (JSON/XML/etc. as strings)
codecs.bytes()         // raw Buffer identity
codecs.json<T>()       // JSON.stringify/parse for any mapped type
codecs.int64()         // bigint as fixed-width 8-byte LE
codecs.custom(enc,dec) // protobuf, msgpack, application framing
textMetadata({k:"v"})  // text Metadata map -> wire Buffers
```

### Reading — durable Readers and ad-hoc fetches

A **Reader** owns a durable, server-side cursor — the progress identity
that survives restarts, exactly like a committed Kafka group offset but
without the group protocol:

```ts
const reader = await shop
  .createReader("orders")
  .readerId("fulfillment")                       // durable consume identity
  .readerStartPoint(ReaderStart.BEGINNING)       // first-declare start position
  .maxPageSize(256)                              // max records per fetch
  .open();

const page = await reader.fetch();               // up to maxPageSize records
for (const m of page.records) console.log(m.key, m.value.total);
await reader.ack();                              // commits the delivered frontier
await reader.close();
```

Two SDK-owned loops sit on top of the same fetch→ack cycle — the
event-loop driven `for await` iterator and `reader.forEach(handler)`:

```ts
for await (const m of reader) handle(m.value);        // async iterator
await reader.forEach(async (m) => handle(m.value));   // per-record pump
// or stay manual: fetch() → handle records → ack()
```

For ad-hoc reads without a durable identity, `feed.records(limit)`,
`feed.fetch(after, limit)` (`tail`/`wait_ms` long-poll,
`after_event_time_ns` seeks), and `feed.load(start, sink)` one-shot
replays cover committed history.

### Shared progress — subscriptions and members

A named **Subscription** is the durable shared frontier cooperating
readers claim work against — the Kafka consumer-group model, minus the
rebalance storm. `subscriptionMember(name)` joins a member and runs the
work loop:

```ts
const member = shop.subscriptionMember("orders-sub");
await member.join("member-1");               // register this member
const work = await member.claim();           // RF3-fenced work lease
const page = await member.fetch(work, 256);  // records for this member
await member.ack(work, page);                // commits shared progress
await member.release(work);                  // or transfer to another member
```

Leases are fenced — a lost or expired claim is refused, and work
transfers cleanly to another member.

### The read loop — consumers

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
- `.startAt(...)`, `.maxPageSize(n)`, `.backoff(ms, ms)`, `.onError(fn)`,
  `.lastError`, `.isRunning`, `runAsync(handler)` tune and observe it;
  `feed.subscribe` remains for page-oriented control.

A complete enrichment service — durable consumer, named-key fallback,
DLQ, idempotent outputs, counters — is
[examples/enrich-users.mjs](examples/enrich-users.mjs).

### Writing

```ts
// Declares + opens the shop.ingest session; each append resolves after
// majority commit (at least 2 of 3 Nodes):
const writer = await orders.writer("ingest");
const appended = await writer.append("order-42", { id: "order-42", total: 99 });
// appended: messageId, cursor, deduplicated, durability — the ack payload

// Idempotent events: pin requestId for caller-driven retries —
// the same identity replays its committed result instead of writing twice.
await writer.appendEvent({
  requestId: "a-stable-request-uuid",   // retry-safe identity
  key: "order-42",
  value: { id: "order-42", total: 99 },
  metadata: textMetadata({ source: "api" }),
});

// One commit for a group of records:
await writer.appendAll([{ requestId: rid, key: "o2", value: order2 }]);
```

Every append carries an idempotent `requestId` — retrying a timed-out
append with the same identity deduplicates; reusing it with different
contents is refused. Writers are epoch-fenced sessions: a second writer
claiming `ingest` fences the first, so an ambiguous timeout can never
double-commit under retry. `useCompression` opts into LZ4; record
`eventTimeNs`/`ingestTimeNs` are `bigint` — the full signed 64-bit
nanosecond range round-trips exactly (never imprecise JS numbers).

## Working with StateStores

StateStores are durable, replicated key/value views that live on the
Nodes — the SDK holds no local copy. Declare named keys up front and
each becomes a maintained secondary index.

### Declaring a store — named keys

```ts
const accounts = await client.createDomain("accounts");
const spec = StateStore.named("employees")         // -> accounts.employees
  .addUniqueKey("passport")                        // one row per value
  .addNonUniqueKey("department");                  // value → many rows; up to 10 keys
const store = await accounts.defineStateStore(spec);
```

**Unique** (`passport`, `email`, `order_number`) — one value maps to at
most one row; `getBy` returns the single match and a second claim on the
same value arbitrates first-committed-wins. **Non-unique**
(`department`, `status`, `region`) — one value maps to many rows;
`getBy` returns every match as a bounded page.

Two store kinds: **manual** (your app puts rows — no source Feed) and
**feed-sourced** (the Nodes replay a source Feed and project rows +
postings via `Extractor.recordKey()` / `Extractor.metadata(k)` /
`Extractor.json(path)` — required on feed-sourced keys, refused on
manual ones).

### Putting and deleting

Named-key postings take a typed `StoreKeySet`, a builder callback, or a
plain record:

```ts
const profiles = accounts.store("employees", codecs.json<Profile>());

await profiles.put("emp-1", payload, PassportKeySet.with("P123", "eng")); // typed key set
await profiles.put("emp-1", payload,
  { passport: "P123", department: "eng" });          // raw record — no checking

await store.delete("emp-1");        // mutation id generated
await store.clear();                // one journaled ClearAll
```

Every mutation carries an idempotent `mutationId` — generated per call,
or pinned for caller-driven retries.

### Reading

```ts
const row = await store.get("emp-1");            // StoreRow | null + freshness evidence
const user = await users.find(email);            // value or null — skips the row wrapper
const user2 = await users.findBy("user_id", id); // first named-key match
const count = await counters.getOrDefault(bucket, 0);  // counters, one line

const rows = await store.getBy("passport", "P123");    // every match
const page = await store.page(afterKey, 256);          // one bounded page
for await (const r of store.iterate()) export(r);      // primary-key order
```

`get`/`getBy` return rows carrying applied-cursor freshness evidence —
`strict` bounded-waits for the replica to apply its committed frontier
rather than answering stale. The byte-level `domain.stateStore(name)`
facade remains for raw access; `domain.feedOps()`, `domain.writer`, and
`domain.reader` keep byte-level sessions. See
[docs/stores.md](docs/stores.md) for the full store guide.

## Working with Pipes

Managed Pipes are server-run stream jobs: you declare the business
shape — source Subscription, output Feed, operation — and the server
owns the subscription frontier, compute lease, panes, and the effect
journal. Every endpoint is a `ScopedRef`: a bare string (or
`scoped.rel`) resolves in-domain; `scoped.abs(...)` or a
`Feed`/`Store` handle crosses Domains explicitly. The Pipe's own name
and its input Subscription always stay in the declaring Domain. See
[docs/pipes.md](docs/pipes.md) for the full pipe guide.

### Simple — forwarding

```ts
const orders = client.domain("orders");
await orders.defineForwardPipe("relay",
  "orders-sub",     // source Subscription
  "relayed");       // output Feed, same Domain
```

### Enrichment — StateStore lookups

```ts
await orders.defineEnrichPipe("enrich", "orders-sub",
  "enriched", scoped.abs("crm.customers"),   // cross-Domain lookup store
  PipeOperation.enrich(Extractor.recordKey(),  // join on record key
    { freshness: "latest", onMiss: "quarantine" }),  // misses → DLQ
  "enrich-dlq");
```

### Time-based — windowing

```ts
await orders.defineWindowPipe("hourly", "orders-sub",
  scoped.abs("stats.orders-hourly"),
  PipeOperation.window(Time.h(1),  // 1h panes
    Extractor.recordKey(), WindowAggregation.count(),
    { time: "event",
      watermark: Time.m(5),   // 5m horizon
      grace: Time.s(30),      // 30s late revisions
      expire: Time.h(2) }),   // pane expiry
  "hourly-emitted");
```

### Indexes and observability

```ts
// Pipe lifecycle: describe, pause, resume, drop.
const pipe = orders.pipe("relay");
const info = await pipe.describe();    // stage, operation, outputs, status
await pipe.pause(); await pipe.resume(); await pipe.drop();
const pipes = await orders.pipeInfos();    // the declared catalog, in-domain

// Index queries: exact, prefix, or range predicates over decoded rows.
const page = await crm.queryIndex(
  "byregion",
  indexPredicate.range(
    ["emea"], indexBound.inclusive(0), indexBound.exclusive(99)),
  { limit: 50, strict: true });   // strict: wait for committed target
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

## Errors, cursors, and time

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...) — `Refused` `StaleSession`
`AmbiguousResult` `Conflict` `Unavailable` `LeaseConflict`.
`Unavailable` covers transient refusals; `AmbiguousResult` marks
operations that may have committed — retry through the same
`requestId`, never blindly re-issue.

Cursors are branded opaque strings: never parsed, ordered, or
fabricated. `eventTimeNs`/`ingestTimeNs` are `bigint` — the full signed
64-bit nanosecond range round-trips exactly.

## Build

```bash
npm install
npm run build       # tsc -> dist/ (JS + .d.ts)
npm test            # vitest: unit (+ conformance when env is set)
```

## Unit-test doubles

`createFakeRiverbed()` serves the wire contract over loopback `node:http`
— request-id dedupe, ack-gated redelivery, epoch fencing, modeled
StateStores — so application tests run the real client with no Riverbed.

```ts
import { createFakeRiverbed } from "whitewater-client";
const riverbed = await createFakeRiverbed();
const client = new WhitewaterClient({ endpoints: riverbed.endpoints, apiKey: "test" });
// ...
await riverbed.close();
```

See `test/testing-double.test.ts` and the
[testing guide](../whitewater/docs/guides/testing-your-app.md).

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

## Full API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`commands`, `wcl`, and the `*SessionClient` classes are the explicit
escape hatches for the same wire contract.

| Method | Parameters | Description |
|---|---|---|
| **Client & transport** | | |
| `new WhitewaterClient({ apiKey, endpoints, timeout? })` | options object | Client construction |
| `healthy()` `useCompression` | — | Liveness, LZ4 on feeds/writers |
| **Domains — the only door** | | |
| `createDomain(name)` / `domain(name)` / `getDomain(name)` | relative name | Declare / lazy / eager handle |
| `feed(name[, codec])` `getFeed` | name, `codecs.*` | Typed `Feed<K,V>` handle |
| `createFeed` `createReader` `createWriter` `createSubscription` `subscribe` | relative name | Declare + open inside the Domain |
| `feeds(prefix)` `feedNames()` `readers()` `writers()` `subscriptions()` `pipes()` `indexes()` `stateStores()` | optional prefix | Domain catalog views |
| `domainId` `revision` `describe()` `historyPolicy()` `refresh()` | — | Domain metadata |
| `setDomainSchema` `setFeedSchema` `clearDomainSchema` `clearFeedSchema` `schema` `feedSchema` `declaredFeedSchema` | `{ format, subject }` — `json`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` | Record-schema declarations |
| **Feeds** | | |
| `records` `fetch` `load(start, sink)` | limit, cursor, `ReaderStart` | Committed history reads |
| `writer` `reader` `subscribe` `subscribeDecoded` `consumer` | name, options | Bound sessions + loops |
| **Writing** | | |
| `FeedWriter.append(k, v)` `appendEvent` `appendAll` | key, value, `FeedEvent` | Write resolving on majority commit — `AppendResult` |
| `sessionEpoch` `useCompression` | — | Session + lifecycle |
| **Reading** | | |
| `FeedReader.fetch(limit)` `ack()` `ackCursor` `close()` `sessionEpoch` | page bound, cursor | Manual durable-Reader loop |
| `for await (const m of reader)` `forEach(handler)` | record handler | Event-loop read loops |
| `ReaderStart` | `BEGINNING` `END` `now()` `cursor` `timestamp` | First-declare start position |
| `feedOps().temporaryFetch` `records` `load` | `after_event_time_ns`, `tail`, `wait_ms` | Ad-hoc + replay reads |
| `feed.consumer(name)` → `FeedConsumer` | `startAt` `maxPageSize` `onRecord` `onError` `backoff` `lastError` `isRunning` | Supervised durable consumer: `run`/`runAsync`/`close` |
| **Subscription members** | | |
| `subscriptionMember(name)` | `join` `claim` `renew` `release` `fetch` `ack` `transfer` | Shared progress + RF3 work leases |
| `feed.subscribe` `subscribeDecoded` `domain.subscribe` | reader, feed, handler | Managed pump loops |
| **StateStores** | | |
| `StateStore.named(name)` `defineStateStore` `dropStateStore` `stateStore(name)` `store(name)` | spec, codecs, source Feed | Store lifecycle + typed handle |
| `put` `delete` `clear` | key, value, named-key postings | Mutations |
| `get` `getBy` `find` `findBy` `getOrDefault` `page` `iterate` | key, named key, `strict`/`limit`/`after` | Row + named-key reads |
| `addUniqueKey` `addNonUniqueKey` `namedKeys` `drop` `view` `raw` | key name | Spec + administration |
| **Indexes** | | |
| `queryIndex(name, predicate, opts)` | `indexPredicate.exact/prefix/range` + `indexBound.inclusive/exclusive`, `{ limit, strict }` | Decoded index rows |
| **Pipes** | | |
| `defineForwardPipe` `defineEnrichPipe` `defineWindowPipe` | name, `ScopedRef` (`scoped.rel`/`abs`/handles), `PipeOperation` + `WindowAggregation`/`WindowTime` | Managed jobs (`Time.h/.m/.s/.ms/.us/.d/.ns` durations) |
| `pipe(name)` `pipeInfos()` | — | `describe` `pause` `resume` `drop` |
| **Catalog lifecycle** | | |
| `create*` `describe*` `rename*` `drop*` `list*` `openWriterSession` `seekReader` | names | Domain/Feed/Writer/Reader/Subscription/Role lifecycle + `listGrants` |
| **Observability** | | |
| `demandFeeds` `demandReaders` `demandSubscriptions` `demandIndexes` `demandPipes` `hotKeys` `demandHotKeys` | optional feed | `DemandReport`/`DemandRow` + `HotKey` evidence |
| **Escape hatches & errors** | | |
| `wcl(script)` `commands([...])` `WriterSessionClient` `ReaderSessionClient` `SubscriptionMemberClient` `FeedClient` | script, raw commands | Byte-level wire access |
| `WhitewaterError` | `code` | `Refused` `StaleSession` `AmbiguousResult` `Conflict` `Unavailable` `LeaseConflict` |
| `createFakeRiverbed()` | — | Loopback wire-contract test double |

# whitewater-node

Node.js/TypeScript SDK for Whitewater Riverbeds. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Node 20+. No runtime dependencies — transport uses the global
`fetch`; bytes are `Buffer`; nanosecond times are `bigint` serialized as
decimal strings (never imprecise JS numbers).

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

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
| `client` (direct) | WCL + typed command execution (`/v1/admin/wcl`, `/v1/admin/commands`), node health, demand observability, and Domain/Role lifecycle (`createDomain`, `domain`, `getDomain`, `describeDomain`, `renameDomain`, `dropDomain`, `listDomains`, `createRole`, …). `ReaderStart` (`beginning()`, `now()`, `cursor(c)`, `timestampNs(ns)`) supplies `START AT` positions |
| `client.createDomain(name)` → `Domain` / `client.domain(name)` / `client.getDomain(name)` | **The only front door for resources** — `createDomain()` returns the Domain handle; `domain()` is lazy (free name composition, no I/O); `getDomain()` verifies and materializes the catalog in one batched read — `feeds("order")`, `feedNames()`, `readers()`, `writers()`, `subscriptions()`, `pipes()`, `indexes()`, `stateStores()`, `describe()`, `historyPolicy`, `refresh()` |
| `domain.createFeed` / `feed` / `createReader` / `createWriter` | Feeds and typed `Feed<K,V>` binding (`feed("orders", codec?)`) — `writer()`/`reader()` auto-declare and open sessions; fluent `createReader`/`createWriter` builders take relative feed names |
| `domain.writer` / `reader` / `memberClient` / `subscription` | Named-Writer/Reader sessions and raw subscription-member leases, scoped inside the Domain |
| `domain.createSubscription` / `subscribe` | Durable Subscriptions and the managed consume loop — the source Feed may cross Domains via a `Feed` handle or `scoped.abs(...)` |
| `domain.stateStore` / `store` / `defineStateStore` / `dropStateStore` | Server-side StateStore facade — durable RF3 rows on the Nodes: `put`/`get`/`getBy`/`iterate`/`delete`/`clear`/`drop`, codec-bound `Store<K,V>` |
| `domain.pipe` / `pipeInfos` / `defineForwardPipe` / `defineEnrichPipe` / `defineWindowPipe` | Managed Pipes — every endpoint a `ScopedRef` (`scoped.rel`/`scoped.abs`/handles); Pipe + input Subscription stay in-domain |
| `domain.queryIndex` | Replicated Index queries — exact/prefix/range predicates |
| `domain.describeFeed`/`describeWriter`/`describeReader`/`describeSubscription`, `rename*`, `drop*`, `setFeedSchema`/`clearFeedSchema`, `openWriterSession`, `seekReader` | Resource lifecycle — every name resolves inside the Domain |

### Typed feeds and stores — codecs do the serialization

Bind a feed or store to codecs once; application code then works in its
own types — never manual `Buffer.from`/`JSON.parse` on every call:

```ts
import { codecs } from "whitewater-client";

interface Order { id: string; total: number }

// String keys + JSON payloads (one arg types the payload; two args
// bind key and payload codecs explicitly).
const ordersDomain = await client.createDomain("orders");
const orders = ordersDomain.feed("events", codecs.json<Order>());

const writer = await orders.writer("ingest");   // declares + opens session
await writer.append("order-1", { id: "order-1", total: 42 });
await writer.appendAll([{ requestId: rid, key: "o2", value: order2 }]);

const reader = await orders.reader("audit", 64); // declares + opens session
const page = await reader.fetch();
for (const m of page.records) console.log(m.key, m.value.total);
await reader.ack();
await reader.close();

const accounts = client.domain("accounts");
const profiles = accounts.store("employees", codecs.json<Profile>());
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

`domain.feed(relative)` binds the default `Feed<string, string>` — UTF-8 both ways, so plain JSON flows with no codec ceremony; `codecs.bytes()`/`codecs.custom` opt into raw/other shapes;
`feed.view(keyCodec, valueCodec)` and `store.view(...)` rebind codecs
without a wire call. The byte-level `domain.feedOps()`,
`domain.writer`, `domain.reader`, and `domain.stateStore` facades
remain for raw access.

### Wire-level API

```ts
const accounts = await client.createDomain("accounts");
const spec = StateStore.named("employees")                          // -> accounts.employees
  .addUniqueKey("passport").addNonUniqueKey("department");           // up to 10 named keys
const store = await accounts.defineStateStore(spec);               // durable store on the Nodes

// named-key postings: a typed StoreKeySet, a builder callback, or a plain record —
await store.put("emp-1", payload, PassportKeySet.with("P123", "eng"));
await store.put("emp-1", payload, { passport: "P123", department: "eng" });
const row = await store.get("emp-1");              // StoreRow | null + freshness
const rows = await store.getBy("passport", "P123"); // named key — non-unique, a list
for await (const r of store.iterate()) export(r);
```

The store lives on the Nodes — the SDK holds no local copy. See
[docs/stores.md](docs/stores.md) for the full store guide and
[docs/pipes.md](docs/pipes.md) for managed stream jobs
(forward/enrich/window) built on subscriptions and stores.

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...). `Unavailable` covers transient refusals;
`AmbiguousResult` marks operations that may have committed — retry
through the same `requestId`, never blindly re-issue.

Cursors are branded opaque strings: never parsed, ordered, or
fabricated. `eventTimeNs`/`ingestTimeNs` are `bigint` — the full signed
64-bit nanosecond range round-trips exactly.

## Typed Domains — navigate, don't concatenate

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so the
friendliest surface navigates domain → feed → reader/writer instead
of composing dotted names by hand:

```ts
import { ReaderStart, WhitewaterClient } from "whitewater-client";

const shop = await client.getDomain("shop");   // verifies + materializes now
// client.domain("shop") is the lazy form — zero I/O until needed
await shop.createFeed("orders");           // CREATE FEED shop.orders;

await shop.feedNames();                        // ['shop.orders', 'shop.returns']
(await shop.feeds("order")).map((f) => f.name); // prefix-filtered Feed handles
await shop.readers(); await shop.writers();     // scoped catalog views

const orders = shop.feed("orders", codecs.json<Order>());

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
const writer = await shop
  .createWriter("orders")
  .writerId("ingest")      // resolves to shop.ingest
  .open();
await writer.append("order-1", { id: "order-1", total: 42 });

const reader = await shop
  .createReader("orders")
  .readerId("fulfillment")                        // durable id — the Kafka groupId analogue
  .readerStartPoint(ReaderStart.BEGINNING)        // first-declare only
  .maxPageSize(256)
  .open();
await reader.forEach(async (m) => handle(m.value)); // per-record pump: deliver → ack each page
// or iterate: `for await (const m of reader)` — same ack-per-page semantics
// or stay manual: fetch() → handle records → ack()
await reader.close();
```

`readerId` is durable application state: `readerStartPoint` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier (the `auto.offset.reset` mapping). A `Feed` bound
to another Domain is refused locally, before any network call. The
first catalog accessor issues ONE batched control read (`describe
domain` + every `show` listing) and caches the snapshot — `refresh()`
re-pulls.

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
await shop.refresh();                    // the snapshot cached the catalog

await shop.schema();                     // the Domain default
await shop.feedSchema("orders");         // effective: key=text, value=json/order-v1
await shop.declaredFeedSchema("orders");
await shop.clearFeedSchema("orders");    // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`,
or `custom.<name>` — compatibility enforcement is still a no-op.

## Permanent consumers — the Kafka loop without the plumbing

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

## Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free model
— names, never partitions or Nodes:

```ts
// Pipe declaration: the client names the business key, source and
// outputs — the server owns panes, leases and the effect journal. Every
// endpoint is a ScopedRef: a bare string (or scoped.rel) resolves
// in-domain; scoped.abs(...) or a Feed/Store handle crosses Domains —
// the Pipe's name and input Subscription always stay in-domain.
const orders = client.domain("orders");
await orders.defineForwardPipe("relay", "orders-sub", "relayed");
await orders.defineEnrichPipe("enrich", "orders-sub",
  "enriched", scoped.abs("crm.customers"),   // lookup store lives in crm
  PipeOperation.enrich(Extractor.recordKey(),
    { freshness: "latest", onMiss: "quarantine" }),
  "enrich-dlq");
await orders.defineWindowPipe("hourly", "orders-sub",
  scoped.abs("stats.orders-hourly"),
  PipeOperation.window(Time.h(1),  // 1h panes
    Extractor.recordKey(), WindowAggregation.count(),
    { time: "event",
      watermark: Time.m(5),   // 5m horizon
      grace: Time.s(30),      // 30s late revisions
      expire: Time.h(2) }),
  "hourly-emitted");

// Pipe lifecycle: describe, pause, resume, drop.
const pipe = orders.pipe("relay");
const info = await pipe.describe();    // stage, operation, outputs, status
await pipe.pause(); await pipe.resume(); await pipe.drop();
const pipes = await orders.pipeInfos();    // the declared catalog, in-domain

// Index queries: exact, prefix, or range predicates over typed values —
// indexes stay same-Domain with their Feed.
const crm = client.domain("crm");
const page = await crm.queryIndex(
  "byregion",
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

## API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`commands`, `wcl`, and the `*SessionClient` classes are the explicit
escape hatches for the same wire contract.

| Area | API |
|---|---|
| Client & transport | `new WhitewaterClient({ apiKey, endpoints, timeout? })` · `healthy()` · `useCompression` on feeds/writers |
| Typed Domains | `domain(name)` lazy / `getDomain(name)` eager · `domainId` `revision` `describe()` `historyPolicy()` `refresh()` · `feeds(prefix)` `feedNames()` `readers()` `writers()` `subscriptions()` `pipes()` `indexes()` `stateStores()` · `feed(relative[, codec])` `getFeed` · `createFeed` `createReader` `createWriter` (relative/Feed) |
| Schemas | `setDomainSchema` `setFeedSchema` `clearDomainSchema` `clearFeedSchema` · `Domain.schema()` `feedSchema()` `declaredFeedSchema()` · `SchemaDecl`/`FeedSchema` — `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` |
| Feeds | `feed(name[, keyCodec, valueCodec])` → `Feed<K,V>` · `useCompression` · `records()` `fetch()` `load()` · `writer` `append` `reader` `subscribe` `subscribeDecoded` `consumer` |
| Writing | `FeedWriter.append(key, value)` `appendEvent` `appendAll` · `useCompression` `sessionEpoch` · idempotent `requestId` · `FeedEvent` carries event time + Metadata |
| Reading | `FeedReader.forEach(handler)` pump · `for await (const m of reader)` async iterator — event-loop driven, no threads · `fetch(limit)` `ack()` `ackCursor` `close()` · `sessionEpoch` · durable `readerId` · `ReaderStart` — `BEGINNING`, `END`, `now()`, `cursor`, `timestamp(Date)` |
| Temporary fetches | `Feed.fetch(after, limit)` / `FeedClient.temporaryFetch` — `after_event_time_ns` seeks, `new_only`, `tail` + `wait_ms` long-poll · `Feed.records` `Feed.load` |
| Consumers | `feed.consumer(reader)` → `FeedConsumer`: `startAt` `maxPageSize` `onRecord` `onError` `backoff` · `run` / `runAsync` / `close` |
| Subscription members | `subscriptionMember(name)` → `join` `claim` `renew` `release` `fetch` `ack` — shared progress + RF3 work leases · `feed.subscribe`/`subscribeDecoded` managed loop |
| StateStores | `store(name)` → `Store<K,V>`: `put` `get` `getBy` `find` `findBy` `getOrDefault` `delete` `page` `iterate` (async gen) `clear` `drop` `raw` · `stateStore(name)` byte-level · `stateStores.define(spec)` `drop` |
| Indexes | `queryIndex(name, IndexPredicate)` → `IndexPage` · `IndexPredicate.exact/prefix/range` + `IndexBound` |
| Pipes | `defineForwardPipe` `defineEnrichPipe` `defineWindowPipe` · `pipe(name)` → `describe` `pause` `resume` `drop` · `pipes()` · `PipeOperation` `WindowAggregation` `WindowTime` · durations are `Time` (`Time.h/.m/.s/.ms/.us/.d/.ns`) |
| Catalog lifecycle | `create*` `describe*` `rename*` `drop*` `list*` for Domain, Feed, Writer, Reader, Subscription, Role · `listGrants` · `openWriterSession` `seekReader` |
| Observability | `demandFeeds` `demandReaders` `demandSubscriptions` `demandIndexes` `demandPipes` → `DemandReport`/`DemandRow` · `hotKeys()` → `HotKey` |
| Escape hatches | `wcl(script)` · `commands([...])` raw command batch · `WriterSessionClient` `ReaderSessionClient` `SubscriptionMemberClient` byte-level sessions · `FeedClient` raw feed ops |
| Errors | `WhitewaterError` — `Refused` `StaleSession` `AmbiguousResult` `Conflict` `Unavailable` `LeaseConflict` · `code` carries the wire `code` |
| Nanoseconds | `event_time_ns`/`ingest_time_ns` are `bigint` or decimal strings — never JS numbers |
| Testing | `createFakeRiverbed()` — loopback HTTP Riverbed implementing the wire contract (dedup, epochs, at-least-once reads, stores, WCL/commands, schemas) |

## Build & test

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

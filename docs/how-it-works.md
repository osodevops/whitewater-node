# How the Node SDK works

Everything a caller can do enters through `WhitewaterClient` — and every
HTTP call funnels through one `Transport`. There are no other network
paths.

## Layers

```
WhitewaterClient ─┬─ wcl / commands / healthy           → POST /v1/admin/*
                  ├─ createFeed / describeFeed / …      → typed WCL builders
                  ├─ feeds   → FeedClient               → /v1/feeds/*, /v1/readers/temporary/fetch
                  ├─ writer(name, epoch)                → /v1/writers/append[-batch]
                  ├─ reader(name)                       → /v1/readers/open|fetch|ack|close
                  └─ subscriptionMember(name)           → /v1/subscriptions/members/*
       │                      │
    Transport            wire.ts (codec)
  (endpoints,         (request building,
   auth, errors)       response mapping)
```

### `Transport`

Holds the endpoint list and API key. `post`/`get` round-robin the
endpoints, attach `authorization: Bearer <key>`, and map every non-2xx
to a `WhitewaterError` subtype by the wire `code` field:
`StaleSession`, `Conflict`, `LeaseConflict`, `Refused`, `Unavailable`
(unreachable/timeout), and `AmbiguousResult` (5xx without a code —
the server may or may not have applied the write; **retry with the
same `requestId`**, never a new one).

### `wire.ts`

The only place request bodies are built and responses decoded. Keys,
payloads, and metadata values are base64 `Buffer`s; `eventTimeNs`/
`ingestTimeNs` travel as decimal strings and surface as `bigint` —
never `number`, so nanosecond precision survives.

`Cursor` is a **branded string**: opaque at the type level, never
parsed or ordered.

## Resource management — two doors, one room

`client.wcl(script)` sends raw WCL to `POST /v1/admin/wcl`. The typed
methods are thin builders over it —
`client.createFeed("orders.events")` serializes
`CREATE FEED orders.events` and calls `wcl` internally.

Two validators run before interpolation, throwing `Error` locally:

- `dotted()` — domains/feeds: `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`,
  ≤512 bytes.
- `resource()` — writers/readers/subscriptions/roles:
  `^[A-Za-z][A-Za-z0-9_-]{0,127}$`.

`ReaderStart` supplies `START AT` positions: `beginning()`, `now()`,
`cursor(c)` (quotes escaped), `timestampNs(ns: bigint)`.

Every management call resolves to `ControlExecution` — `requestId`,
`revision`, `authority`, `warning`, and one `StatementResult` per WCL
statement.

### WCL by example

Raw script — several statements commit as one request (pass a shared
`requestId` to dedupe a retried batch):

```ts
await client.wcl(`
  CREATE DOMAIN shop;
  CREATE FEED shop.orders;
  CREATE WRITER orders.writer TO shop.orders;
  OPEN WRITER SESSION orders.writer;
  CREATE READER audit FROM shop.orders START AT BEGINNING;
  SHOW FEEDS;
`);
```

The same calls through the typed builders — each serializes one WCL
statement and calls `wcl` internally:

```ts
await client.createDomain("shop");                  // CREATE DOMAIN shop;
await client.createFeed("shop.orders");             // CREATE FEED shop.orders;
await client.createWriter("orders.writer", "shop.orders");
await client.openWriterSession("orders.writer");    // epoch in .data
await client.createReader("audit", "shop.orders", ReaderStart.beginning());
await client.listFeeds();                           // SHOW FEEDS;
await client.seekReader("audit", ReaderStart.now());// SEEK READER ... TO NOW;
await client.dropFeed("shop.orders");               // DROP FEED shop.orders;
```

`client.commands([...])` is the second door — the typed JSON command
path for what WCL does not spell. Declaring a StateStore with named
keys is the current example (there is no `DEFINE STATESTORE` WCL
statement yet):

```ts
await client.commands([{
  command: "define_state_store",
  name: "accounts.employees",
  source: { kind: "manual" },
  named_keys: [
    { name: "passport", unique: true },
    { name: "department", unique: false },
  ],
}]);
```

(`client.stateStores.define(spec)` posts exactly this body for you.)

`DROP` is spelled in WCL for every resource including stores:
`await client.wcl("DROP STATESTORE accounts.employees")` ≡ `store.drop()`.

## Reading

Three shapes, in increasing durability:

1. **`feeds.records(feed, limit, after)`** — one-shot committed history.
2. **`feeds.temporaryFetch(feed, { tail: true, waitMs, after, limit })`** —
   anonymous ephemeral reader; `tail` + `waitMs` is a server-side
   long-poll that returns early when records arrive. Continue with
   `nextCursor`, never the last record's cursor.
3. **`client.reader(name)`** — durable independent cursor:
   `open` → `fetch` → `ack(deliveredCursor)` → `close`. Progress
   survives restarts.

Two managed wrappers sit on top of those primitives:

- **`feeds.load(feed, start, sink)`** — one-shot bulk read: pages
  `temporaryFetch` from `ReaderStart` until the frontier is drained.
  `sink` is any `(records) => void` callback.
- **`client.subscribe(reader, feed, start, onRecords[, onError])`** — a
  durable consume loop in the background: provisions the Reader when
  missing, then `open` → [`fetch` → `onRecords` → `ack`] → `close`.
  Transient errors retry; `StaleSession` and hard refusals terminate via
  `onError`/`subscription.failure`. `await sub.close()` stops it.

`client.subscriptionMember(sub)` is the lease vocabulary for
cooperating consumers: `join` → `claim`/`renew`/`release` → `fetch` →
`ack`.

## Writing

- **Named session**: `openWriterSession(name)` returns a
  `session_epoch` in `results[0].data`; bind `client.writer(name,
  epoch)` and `append` with a stable `requestId` per logical write.
- **Ad-hoc**: `feeds.append(feed, feedAppend)` — ephemeral writer
  identity; same `requestId` dedup rule.

## Testing

Unit tests stub the wire with `node:http`, asserting serialized
statements and response mapping — no live Riverbed required. The
conformance suite (`test/conformance.test.ts`) replays the shared
`sdk/spec/conformance` fixtures when `WHITEWATER_ENDPOINTS` +
`WHITEWATER_SPEC_DIR` are set; skipped otherwise.

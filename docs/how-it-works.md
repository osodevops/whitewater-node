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
  `sink` is a `RecordStore` or any `(records) => void` callback.
- **`client.loadStore(store).fromFeed(feed, start, projection)`** — the
  same bulk read with a per-record projection: `RecordStore` declares up
  to 10 named secondary keys (`addKey`), and the callback writes each
  row via `store.upsert(record).withKey(k).withNamedKey(n, v)`. Store
  ops: `get(key)`, `getBy(namedKey, value)` (non-unique — returns a
  list), `delete`, `clear`, `copy(target)`, `forEach`,
  `forEachParallel(concurrency, fn)` — async fan-out, not CPU threads.
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

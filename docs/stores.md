# RecordStore and loadStore — coding guide

A `RecordStore` is a named, thread-safe, in-memory projection of Feed
records. Fill it once with `loadStore` (bulk history) and keep it warm
with `subscribe` (live tail) — the Whitewater equivalent of a local
materialized view.

## Store anatomy

Two structures per store:

- **Primary map** — `primary key -> FeedRecord`. One row per key; a new
  row for an existing key replaces it.
- **Named-key posting maps** — one per declared named key, conceptually
  `employee-store.passport`: `named value -> set of primary keys`.
  Named keys are **not unique** — many rows may share one named value,
  so lookups return an array.

```ts
const store = RecordStore.named("employee-store")
  .addKey("passport")           // declare before use
  .addKey("department");        // at most RecordStore.MAX_NAMED_KEYS (10)
```

`addKey` returns the store for chaining. An eleventh key throws
`Error("a RecordStore supports at most 10 named keys")`; blank names
throw `Error("named key is required")`.

## Loading history: `client.loadStore(store).fromFeed(...)`

```ts
const loaded = await client.loadStore(store).fromFeed(
  "hr.employees",
  ReaderStart.beginning(),              // or .now(), .cursor(c), .timestampNs(ns)
  async (s, record) => {
    s.upsert(record)
      .withKey(record.key ?? Buffer.alloc(0))
      .withNamedKey("passport", record.metadata["pp"] ?? Buffer.alloc(0))
      .withNamedKey("department", "eng"); // string overloads are UTF-8
  });
```

Semantics, in order:

1. Pages `temporaryFetch` from `start` until the frontier is drained —
   a **one-shot bounded read**, not a tail.
2. For every record the projection callback runs `store.upsert(record)`.
   The callback may be `async` — it is awaited per record.
3. `withKey(Buffer|string)` chooses the primary key. Defaults to
   `record.key`; pass anything — a composite `"dept:emp-1"` — and the
   row re-keys (old postings are stripped first).
4. `withNamedKey(name, Buffer|string)` attaches one named-key value;
   call it once per declared key you want indexed. An **undeclared**
   named key throws `Error("undeclared named key: ...")` — a projection
   bug fails the load, never silently writes an unindexed row.
5. Resolves to the count of records read (not rows written — re-keys
   replace, they don't duplicate).

Skip `withNamedKey` and the row lands in the primary map only — fully
readable, just not indexable by that named key.

`feeds.load(feed, start, sink)` remains the lighter "dump history into
a store or callback" path; `sink` may be the store itself (rows key on
`record.key`, cursor fallback for unkeyed records) or a `(records) =>`
batch callback.

## Keeping the store warm: `subscribe`

```ts
const sub = client.subscribe("hr-view", "hr.employees",
  ReaderStart.now(),                              // only new records
  (records) => {                                  // per fetched page
    for (const r of records) {
      store.upsert(r)
        .withKey(r.key ?? Buffer.alloc(0))
        .withNamedKey("department", r.metadata["department"] ?? "");
    }
  },
  (error) => console.error("subscription ended:", error));
// ...later
await sub.close();
```

Delivery is **at-least-once**: records land in the callback, then the
loop acks `deliveredCursor`. A crash between callback and ack replays —
make the projection idempotent (upsert semantics already are).

## Reading the store

```ts
store.get("emp-1");                    // FeedRecord | undefined (string = UTF-8)
store.get(keyBuffer);                  // Buffer overload
store.getBy("passport", "P123");       // FeedRecord[] — every match, non-unique
store.getBy("undeclared", "x");        // throws Error("undeclared named key")
store.size;                            // row count
store.namedKeys();                     // declared keys, in order
store.name;                            // "employee-store"
store.entries();                       // StoreEntry[] — { primaryKey, record }
store.forEach(e => export(e.record));  // stable snapshot iteration
for (const e of store.entries()) { /* e.primaryKey (Buffer), e.record */ }
```

## Mutating outside a load

```ts
store.put(record);                     // primary-key upsert, no named keys
store.putAll(records);                 // subscribe-ready sink shape
store.upsert(record).withKey("custom");// same builder as projections
store.delete("emp-1");                 // removes row + all its postings
store.clear();                         // empties everything
```

`delete` and `clear` strip named-key postings too — a deleted row never
lingers in a `getBy` result.

## Copying and parallel work

```ts
const replica = RecordStore.named("employee-backup");
store.copy(replica);                   // rows + named postings; declares
                                       // missing keys on the target
await store.forEachParallel(3, async (entry) => upload(entry.record));
```

`forEachParallel(concurrency, fn)` bounds **in-flight async callbacks**
to `concurrency` — Node's event loop, not CPU threads. Use it to bound
concurrent I/O (HTTP calls, file writes); the callbacks still run on the
event loop, so CPU-heavy work should use `worker_threads` yourself.
`concurrency < 1` throws. Rejects if any callback rejects.

## Error handling reference

| Where | Type | Meaning / remedy |
|---|---|---|
| `addKey` #11 | `Error` | Store key budget (10) exceeded — declare fewer keys. |
| `addKey`/`getBy`/`withNamedKey` undeclared or blank | `Error` | Programming error — declare the key or fix the name. |
| `loadStore` mid-read | `WhitewaterError` (usually `Unavailable`) | Transport refused a page; retry `fromFeed` — re-upserts are idempotent. |
| `subscribe` transient | retried internally | `Unavailable`/`AmbiguousResult` backoff inside the loop. |
| `subscribe` terminal | `onError` + `subscription.failure` | `StaleSession` (another client opened your Reader — pick a unique name or take over deliberately), `Refused`, `Conflict`. The loop is dead; inspect, fix, re-subscribe. |

All `WhitewaterError` subclasses carry `status` and `code` fields —
`instanceof StaleSession` etc. for policy.

## What a RecordStore is **not**

- **Not durable** — it dies with the process. Rebuild is cheap:
  `loadStore` from `ReaderStart.beginning()` (or a snapshot Cursor).
- **Not a server StateStore** — the server-side replicated store model
  is a separate, later seam. This is the client-side projection.
- **Not unique on named keys** — `getBy` always returns an array, even
  when it happens to hold one row.

# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore` is a thin facade over it: it holds no rows, keeps no local
projection, and every operation is an HTTP call with freshness evidence.

```ts
const spec = StateStore.named("accounts.employees")
  .addKey("passport", true)             // unique: one row per value
  .addKey("department", false);         // non-unique: value -> many rows

const store = await client.stateStores.define(spec);   // manual store
// or a bound facade for an already-declared store:
const same = client.stateStore("accounts.employees");
```

`addKey` records declaration intent used by `define` — named keys are
declared at creation; at most `StateStore.MAX_NAMED_KEYS` (10). Adding a
key to an existing store has no effect server-side.

## Writing

```ts
await store.put("user-1", "profile-1", {
  passport: "P111",
  department: "eng",
  salary: 92000n,
  active: true,
  badge: Buffer.from([1, 2]),
  note: null,
});
await store.delete("user-1");            // mutationId auto-generated
await store.delete("user-1", "stable-mutation-uuid");
```

- `put` commits one journaled `put_row` through the store's RF3 quorum
  and resolves to `{ storeId, mutationId, sequence, deduplicated }`.
- Named values are **typed**: `string` → text, `number`/`bigint` → i64
  (must be a safe integer within i64), `boolean` → bool, `Buffer` →
  bytes, `null` → null. An array value carries one typed value per
  posting component. Floats are refused — the wire carries exact
  i64/text/bytes.
- Named values must match the store's declared keys — the server
  refuses undeclared names. `unique` claims arbitrate
  first-committed-wins by journal order.
- `mutationId` is the idempotency identity: retry a timed-out `put` with
  the same ID and it dedupes; reuse it with different contents and it
  conflicts.

## Reading

```ts
const row = await store.get("user-1");                  // StoreRow | null
row?.payload;                                          // Buffer
row?.freshness;                                        // appliedSeq/committedSeq/behind/servedBy

const { rows, freshness } = await store.getBy("department", "eng");
// rows: StoreRow[] — every match; freshness.servedBy: which Node answered
```

`getBy` is the group/list lookup: one non-unique named value resolves
to **every** primary row carrying it. Pass `{ strict: true, waitMs }` to
bounded-wait for the serving replica to apply its committed frontier
rather than answering stale; non-strict reads still report
`freshness.behind` honestly.

## Iterating

```ts
for await (const row of store.iterate(256)) export(row);

const page = await store.page(afterKey, 256);   // one bounded page
page.nextKey;                                    // continuation, or null
page.pinnedSeq;                                  // journal position pinned to
```

`iterate`/`page` ride `/v1/state-stores/iterate` — the same bounded,
key-ordered row set replica catch-up serves — so `pinnedSeq` is the
iteration's freshness evidence.

## Lifecycle

```ts
await store.clear();                     // one journaled ClearAll
await store.drop();                      // or client.stateStores.drop(name)
```

- `clear` empties the store as one durable, idempotent mutation;
  replicas sweep rows and postings in bounded batches.
- `drop` tombstones the definition — the name cannot be recreated and
  every data path refuses. Rows already applied on replicas remain
  until a bounded reclaim pass.
- Feed-sourced stores: `client.stateStores.define(spec, "accounts.events")`
  materializes committed Feed history; named keys require a manual
  store until declared projections exist.

## What a StateStore facade is **not**

- **Not a local store** — there is no in-process copy to drift from
  the Nodes. Every read goes to a replica; freshness evidence tells
  you how current it was.
- **Not unique on non-unique keys** — `getBy` always returns a list.

## Errors

| Where | Type | Meaning / remedy |
|---|---|---|
| `addKey` #11 / duplicate / blank | `Error` | Declaration contract (10-key bound, distinct names). |
| `define` spec + `source` + named keys | `Error` | Named keys need a manual store — checked before the call. |
| Data op on unbound spec | `Error` | `StateStore.named` builds a spec; bind via `define` or `client.stateStore(name)`. |
| Unsafe/non-integer numeric value | `Error` | The wire carries exact i64 — pass a safe integer or bigint within range. |
| Undeclared named key / missing store | `Refused` 400 | Server-side contract — fix the declaration. |
| `mutationId` reused differently | `Refused` 409 | Retry with the original contents or a fresh ID. |
| No quorum / replica down | `Unavailable` 503 | Retry — the journal needs a quorum to commit. |
| `strict` read can't catch up | `Refused` 409 `index_behind` | Bounded wait expired; retry or drop `strict`. |

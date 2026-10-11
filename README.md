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
| `client.subscribe(reader, feed, start, onRecords)` | Managed durable consume: provisions the Reader, runs open → fetch → deliver → ack in the background; `await sub.close()` to stop. `RecordStore` is a ready-made keyed sink |

```ts
const store = RecordStore.named("employee-store")
  .addKey("passport").addKey("department");                        // up to 10 named keys

await client.loadStore(store).fromFeed("hr.employees", ReaderStart.beginning(),
  (s, record) => s.upsert(record)                                  // bulk fill, projected
    .withKey(record.key!)
    .withNamedKey("passport", record.metadata.get("pp") ?? ""));

store.get("emp-1");                    // by primary key
store.getBy("passport", "P123");       // by named key — non-unique, returns a list
await store.forEachParallel(3, async (entry) => export(entry.record));

const sub = client.subscribe("hr-view", "hr.employees", ReaderStart.now(),
  (records) => store.putAll(records));                                    // keep it warm
await sub.close();
```

`client.feeds.load(feed, start, sink)` remains the lighter one-shot
read when you don't need named-key projections.

Errors surface as `WhitewaterError` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, ...). `Unavailable` covers transient refusals;
`AmbiguousResult` marks operations that may have committed — retry
through the same `requestId`, never blindly re-issue.

Cursors are branded opaque strings: never parsed, ordered, or
fabricated. `eventTimeNs`/`ingestTimeNs` are `bigint` — the full signed
64-bit nanosecond range round-trips exactly.

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

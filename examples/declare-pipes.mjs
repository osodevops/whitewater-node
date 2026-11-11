import {
  Extractor,
  PipeOperation,
  StateStore,
  Time,
  WhitewaterClient,
  WindowAggregation,
} from "../dist/index.js";

/**
 * Declaring managed Pipes — the typed `define_pipe` surface. A Pipe
 * drives one Subscription's delivered records through a server-side
 * operation, with replay-safe journaled effects:
 *
 *   forward — copy each delivered record to another Feed unchanged
 *   enrich  — point-lookup a StateStore row, copy its payload into the
 *             output record's Metadata (enrich.payload / enrich.matched)
 *   window  — accumulate records into fixed event-time panes of a
 *             managed StateStore view; closed panes can EMIT to a Feed
 *
 * Pipes are declared inside a Domain — the Pipe's name and its input
 * Subscription always live there; endpoints are ScopedRefs: a bare
 * string resolves in-domain, `scoped.abs("other.name")` or a Feed/Store
 * handle crosses Domains. Lifecycle is describe/pause/resume/drop on
 * domain.pipe(name).
 *
 * Prerequisites: the dev cluster on localhost:7371-7373. The example
 * declares its own Domain, Feeds, StateStores, and Subscriptions, so it
 * is self-contained — rerun on a fresh cluster (or rename the Domain).
 */

const client = new WhitewaterClient({
  apiKey:
    process.env.WHITEWATER_API_KEY ??
    "whitewater-local-development-admin-key",
  endpoints: [
    "http://localhost:7371",
    "http://localhost:7372",
    "http://localhost:7373",
  ],
});

// ---- topology ---------------------------------------------------------
// One Domain owns every resource; each Pipe gets its own Subscription
// (a Pipe owns its Subscription's driving frontier).

const pipes = await client.createDomain("pipes");
await pipes.createFeed("in");
await pipes.createFeed("out");
await pipes.createFeed("enriched");
await pipes.createFeed("dlq");
await pipes.createFeed("emitted");

// Lookup store for the enrich Pipe (manual — rows arrive via put()).
await pipes.defineStateStore(StateStore.named("profiles"));

// Window pane store: the server writes pane rows keyed by (group, pane)
// and requires non-unique named keys "key" and "pane" so the view is
// enumerable without knowing composite row keys.
await pipes.defineStateStore(
  StateStore.named("panes").addNonUniqueKey("key").addNonUniqueKey("pane"),
);

// Subscriptions are Domain-scoped — one per Pipe, all sourcing pipes.in.
for (const sub of ["flow.copy", "flow.enrich", "flow.agg"]) {
  await pipes.createSubscription(sub, "in");
}

// ---- forward: copy pipes.in -> pipes.out -------------------------------

await pipes.defineForwardPipe("copy", "flow.copy", "out");

// ---- enrich: lookup pipes.profiles by record Key, misses quarantine ----

await pipes.defineEnrichPipe(
  "enrich",
  "flow.enrich",
  "enriched",
  "profiles",
  PipeOperation.enrich(Extractor.recordKey(), { onMiss: "quarantine" }),
  "dlq",
);

// ---- window: 8s event-time panes summing JSON 'amount' per 'customer' --

await pipes.defineWindowPipe(
  "agg",
  "flow.agg",
  "panes",
  PipeOperation.window(
    Time.s(8), // 8s panes — Time extracts the wire nanos internally
    Extractor.json("customer"),
    WindowAggregation.sum(Extractor.json("amount")),
    { maxLate: Time.s(15) },
  ),
  "emitted", // closed panes publish here (requires a lateness bound)
);

// ---- lifecycle ----------------------------------------------------------

const copy = pipes.pipe("copy");
const info = await copy.describe();
console.log("describe pipes.copy:", info.stage, info.operation);

await copy.pause(); // cooperative: in-flight effects finish, idempotent
await copy.resume(); // resumes driving from the durable frontier

for (const pipe of await pipes.pipeInfos()) {
  console.log(`${pipe.name}: stage=${pipe.stage} status=${pipe.status}`);
}

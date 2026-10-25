import {
  Extractor,
  PipeOperation,
  StateStore,
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
 * Pipes are declared by name only — no partitions, no Node placement.
 * Lifecycle is describe/pause/resume/drop on client.pipe(name).
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

await client.createDomain("pipes");
await client.createFeed("pipes.in");
await client.createFeed("pipes.out");
await client.createFeed("pipes.enriched");
await client.createFeed("pipes.dlq");
await client.createFeed("pipes.emitted");

// Lookup store for the enrich Pipe (manual — rows arrive via put()).
await client.stateStores.define(StateStore.named("pipes.profiles"));

// Window pane store: the server writes pane rows keyed by (group, pane)
// and requires non-unique named keys "key" and "pane" so the view is
// enumerable without knowing composite row keys.
await client.stateStores.define(
  StateStore.named("pipes.panes").addKey("key").addKey("pane"),
);

// Subscriptions are dotted resource names — one per Pipe.
for (const sub of ["pipes.flow.copy", "pipes.flow.enrich", "pipes.flow.agg"]) {
  await client.createSubscription(sub, "pipes.in");
}

// ---- forward: copy pipes.in -> pipes.out -------------------------------

await client.defineForwardPipe("pipes.copy", "pipes.flow.copy", "pipes.out");

// ---- enrich: lookup pipes.profiles by record Key, misses quarantine ----

await client.defineEnrichPipe(
  "pipes.enrich",
  "pipes.flow.enrich",
  "pipes.enriched",
  "pipes.profiles",
  PipeOperation.enrich(Extractor.recordKey(), { onMiss: "quarantine" }),
  "pipes.dlq",
);

// ---- window: 8s event-time panes summing JSON 'amount' per 'customer' --

await client.defineWindowPipe(
  "pipes.agg",
  "pipes.flow.agg",
  "pipes.panes",
  PipeOperation.window(
    8_000_000_000, // u64 ns — strict JSON number, never a string
    Extractor.json("customer"),
    WindowAggregation.sum(Extractor.json("amount")),
    { maxLateNs: 15_000_000_000 },
  ),
  "pipes.emitted", // closed panes publish here (requires a lateness bound)
);

// ---- lifecycle ----------------------------------------------------------

const copy = client.pipe("pipes.copy");
const info = await copy.describe();
console.log("describe pipes.copy:", info.stage, info.operation);

await copy.pause(); // cooperative: in-flight effects finish, idempotent
await copy.resume(); // resumes driving from the durable frontier

for (const pipe of await client.pipes()) {
  console.log(`${pipe.name}: stage=${pipe.stage} status=${pipe.status}`);
}

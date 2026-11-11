import {
  Extractor,
  PipeOperation,
  scoped,
  StateStore,
  WhitewaterClient,
} from "../dist/index.js";

/**
 * A Pipe whose endpoints span Domains — purchase orders captured in the
 * "purchasing" Domain relay into the "warehouse" Domain for fulfilment,
 * enriched against the warehouse's own profile store.
 *
 * The rule: the Pipe's NAME and its input Subscription always live in
 * the declaring Domain (the durable frontier is owned there); the
 * Subscription's source Feed and the Pipe's outputs may live in another
 * Domain, referenced through that Domain's handles or
 * scoped.abs("other.name") — an explicit ScopedRef, never ambiguity.
 *
 *   purchasing.orders        — where orders land
 *   warehouse.fulfil         — subscription owned by warehouse,
 *                              sourcing purchasing's feed
 *   warehouse.relay          — the Pipe, owned by warehouse
 *   warehouse.orders.inbound — output feed in warehouse
 *   warehouse.orders.misses  — quarantine feed in warehouse
 *   warehouse.profiles       — lookup store in warehouse
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

const purchasing = await client.createDomain("purchasing");
const warehouse = await client.createDomain("warehouse");

// Orders land in the purchasing Domain — the handle carries its
// qualified name for the cross-Domain subscription below.
await purchasing.createFeed("orders");
const orders = purchasing.feed("orders");

// The warehouse owns its fulfilment resources.
await warehouse.createFeed("orders.inbound");
await warehouse.createFeed("orders.misses");
await warehouse.defineStateStore(
  StateStore.named("profiles").addUniqueKey("email", Extractor.json("email")),
);

// The Subscription belongs to warehouse — that is where its durable
// progress lives — but its SOURCE FEED is purchasing's, passed as the
// handle (an explicit ScopedRef; scoped.abs("purchasing.orders") says
// the same thing as a string).
await warehouse.createSubscription("fulfil", orders);

// warehouse.relay enriches each purchase order against the warehouse's
// own profile store and drops misses on its own quarantine feed — every
// endpoint in-domain; only the input crossed Domains (inside the
// subscription above).
await warehouse.defineEnrichPipe(
  "relay",
  "fulfil",
  "orders.inbound",
  "profiles",
  PipeOperation.enrich(Extractor.json("customer_id"), {
    named: "email",
    onMiss: "quarantine",
  }),
  "orders.misses",
);
console.log("warehouse.relay declared — purchasing.orders -> warehouse.orders.inbound");

// Same topology with an absolute reference instead of the handle —
// identical wire, equally deliberate.
await warehouse.defineForwardPipe(
  "audit.trail",
  "fulfil",
  scoped.abs("purchasing.orders.archive"),
);
console.log("warehouse.audit.trail declared — Abs() endpoint spelled out");

import {
  SchemaCompatibility,
  SchemaTarget,
  SchemaType,
  SchemaValidation,
  StateStore,
  WhitewaterClient,
} from "../dist/index.js";

/**
 * End-to-end walkthrough of the Whitewater Schema Registry
 * (`whitewater.schema.registry`):
 * - Setting up JSON Schema, Avro, and Protobuf schemas on Feeds (key & message)
 *   and StateStores
 * - Automatic client-side validation on sync `append`, async `write`, and
 *   `StateStore.put` — showing where `SchemaValidation` errors surface
 * - `BACKWARD`, `FORWARD`, and `FULL` schema evolution compatibility checks
 * - Sharing a schema across Feeds with read-only linked targets (`linkSchema`)
 *
 * Run against a local Riverbed (ports 7371–7373):
 *   node examples/work-with-schemas.mjs
 */

const client = new WhitewaterClient({
  apiKey: "whitewater-local-development-admin-key",
  endpoints: [
    "http://localhost:7371",
    "http://localhost:7372",
    "http://localhost:7373",
  ],
});

const shop = await client.createDomain("shop");
await shop.createFeed("orders");
await shop.createFeed("invoices");
const orders = shop.feed("orders");
const invoices = shop.feed("invoices");
const customers = await shop.defineStateStore(StateStore.named("customers"));

// 1. Invalid schema definition is rejected immediately on registration
try {
  await client.updateSchema("shop.orders", "{not-valid-json", SchemaTarget.Message, SchemaType.Json);
} catch (err) {
  if (err instanceof SchemaValidation) {
    console.log("Caught invalid schema definition:", err.violations);
  }
}

// 2. Register JSON Schema on shop.orders (both KEY and MESSAGE)
await client.updateSchema(
  "shop.orders",
  JSON.stringify({ type: "string", pattern: "^ord-[0-9]+$" }),
  SchemaTarget.Key,
  SchemaType.Json,
);
const orderEntry = await client.updateSchema(
  "shop.orders",
  JSON.stringify({
    type: "object",
    required: ["orderId", "total"],
    properties: {
      orderId: { type: "string" },
      total: { type: "number", minimum: 0 },
    },
    additionalProperties: false,
  }),
  SchemaTarget.Message,
  SchemaType.Json,
  SchemaCompatibility.BACKWARD,
);
console.log(`Registered ${orderEntry.key} v${orderEntry.version}`);

// 3. Produce valid & invalid records (sync append + async write)
const writer = await orders.writer("order-ingest");
await writer.append("ord-100", JSON.stringify({ orderId: "ord-100", total: 49.95 }));

try {
  await writer.append("bad-key", JSON.stringify({ orderId: "ord-101", total: 10 }));
} catch (err) {
  if (err instanceof SchemaValidation) {
    console.log("Caught invalid key on append:", err.violations);
  }
}

try {
  await writer.append("ord-102", JSON.stringify({ orderId: "ord-102", total: -5 }));
} catch (err) {
  if (err instanceof SchemaValidation) {
    console.log("Caught invalid message on append:", err.violations);
  }
}

// Async write(): invalid record rejects its Promise without poisoning valid records in the batch
const badPromise = writer.write("ord-103", JSON.stringify({ orderId: "ord-103" }));
const goodPromise = writer.write("ord-104", JSON.stringify({ orderId: "ord-104", total: 120 }));
try {
  await badPromise;
} catch (err) {
  if (err instanceof SchemaValidation) {
    console.log("Caught invalid async write:", err.violations);
  }
}
const goodAck = await goodPromise;
console.log("Valid async write committed:", goodAck.messageId);
await writer.close();

// 4. Avro schema on shop.invoices + BACKWARD / FORWARD / FULL compatibility
const avroV1 = JSON.stringify({
  type: "record",
  name: "Invoice",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "long" },
  ],
});
await client.updateSchema("shop.invoices", avroV1, SchemaTarget.Message, SchemaType.Avro, SchemaCompatibility.BACKWARD);

const avroAddReqNoDefault = JSON.stringify({
  type: "record",
  name: "Invoice",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "long" },
    { name: "currency", type: "string" },
  ],
});
const backCheck = await client.checkSchemaCompatibility(
  "shop.invoices",
  avroAddReqNoDefault,
  SchemaTarget.Message,
  SchemaType.Avro,
);
console.log("Avro BACKWARD check (new required field without default):", backCheck);

// Switch to FORWARD: adding a required field is forward-compatible, dropping 'amount' fails
await client.updateSchemaCompatibility("shop.invoices", SchemaTarget.Message, SchemaCompatibility.FORWARD);
const avroDropAmount = JSON.stringify({
  type: "record",
  name: "Invoice",
  fields: [{ name: "id", type: "string" }],
});
console.log(
  "Avro FORWARD check (dropping required field):",
  await client.checkSchemaCompatibility("shop.invoices", avroDropAmount, SchemaTarget.Message, SchemaType.Avro),
);

// Switch to FULL: adding a field with a default succeeds
await client.updateSchemaCompatibility("shop.invoices", SchemaTarget.Message, SchemaCompatibility.FULL);
const avroGoodV2 = JSON.stringify({
  type: "record",
  name: "Invoice",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "long" },
    { name: "currency", type: "string", default: "EUR" },
  ],
});
const invV2 = await client.updateSchema("shop.invoices", avroGoodV2, SchemaTarget.Message, SchemaType.Avro, SchemaCompatibility.FULL);
console.log(`Evolved ${invV2.key} to v${invV2.version}`);

// 5. Protobuf schema on StateStore shop.customers
const protoV1 = `syntax = "proto3";
message Customer {
  string id = 1;
  string email = 2;
  int32 tier = 3;
}`;
await client.updateSchema("shop.customers", protoV1, SchemaTarget.Message, SchemaType.Protobuf);
await customers.put("cust-1", JSON.stringify({ id: "cust-1", email: "alice@example.com", tier: 2 }));
try {
  await customers.put("cust-2", JSON.stringify({ id: "cust-2", tier: "not-an-int" }));
} catch (err) {
  if (err instanceof SchemaValidation) {
    console.log("Caught invalid StateStore.put payload:", err.violations);
  }
}

// 6. Read-only linked schema across feeds
const audit = await client.createDomain("audit");
await audit.createFeed("orders");
const linked = await client.linkSchema("shop.orders", SchemaTarget.Message, "audit.orders", SchemaTarget.Message);
console.log(`Linked read-only schema ${linked.key} -> ${linked.sourceKey}`);

import { codecs, ReaderStart, WhitewaterClient } from "../dist/index.js";

/**
 * The strongly-typed Domain facade — navigate the namespace instead of
 * composing dotted names by hand.
 *
 *   const shop = await client.getDomain("shop");   // materialized view
 *   const orders = shop.feed("orders");            // typed Feed, zero I/O
 *   const reader = await client.createReader(shop, orders)
 *     .readerId("fulfillment")
 *     .readerStartPoint(ReaderStart.BEGINNING)
 *     .open();
 *
 * Run a Riverbed (scripts/test-*.py bring one up on 7371–7373), then:
 *
 *   node examples/domain-model.mjs
 */

const client = new WhitewaterClient({
  apiKey: "whitewater-local-development-admin-key",
  endpoints: [
    "http://localhost:7371",
    "http://localhost:7372",
    "http://localhost:7373",
  ],
});

await client.createDomain("shop");
await client.createFeed("shop.orders");
await client.createFeed("shop.returns");

// getDomain() verifies + materializes now; domain() is the lazy handle.
const shop = await client.getDomain("shop");

console.log("feeds:", await shop.feedNames());
console.log("order-prefix feeds:", (await shop.feeds("order")).map((f) => f.name));

const orders = shop.feed("orders", codecs.json()); // typed Feed, zero I/O

const writer = await client
  .createWriter(shop, orders)
  .writerId("orders.ingest")
  .open();
await writer.append("order-1", { id: "order-1", total: 42 });

const reader = await client
  .createReader(shop, orders)
  .readerId("fulfillment") // durable id — the Kafka groupId analogue
  .readerStartPoint(ReaderStart.BEGINNING) // first-declare only
  .maxPageSize(256)
  .open();
const page = await reader.fetch();
for (const record of page.records) {
  console.log("read", record.key, record.value);
}
await reader.ack(); // durable progress
await reader.close();

await shop.refresh(); // re-pull the catalog snapshot
console.log("readers:", await shop.readers());
console.log("writers:", await shop.writers());

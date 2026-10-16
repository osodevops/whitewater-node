import { codecs, WhitewaterClient } from "../dist/index.js";

/**
 * The codec-bound Feed<K,V> flow: bind a feed to key/payload codecs
 * once, then append(key, value) encodes and every fetch/records
 * decodes — application code never touches Buffers.
 *
 *   orders.writer("ingest")    — declares the Writer + opens a session
 *   orders.reader("audit", 64) — declares the Reader + opens a session
 *   writer.append / reader.fetch / reader.ack
 */

const client = new WhitewaterClient({
  apiKey: "whitewater-local-development-admin-key",
  endpoints: [
    "http://localhost:7371",
    "http://localhost:7372",
    "http://localhost:7373",
  ],
});

await client.createDomain("demo");
await client.createFeed("demo.orders");

// String keys + JSON payloads — the payload codec is the one argument.
const orders = client.feed("demo.orders", codecs.json());

// Declares "ingest" when missing and opens its epoch-fenced session.
const writer = await orders.writer("ingest");
await writer.append("order-1", { id: "order-1", total: 42 });
await writer.append("order-2", { id: "order-2", total: 7 });

// Declares "audit" at BEGINNING when missing and opens a session;
// fetch() returns decoded messages — Order, not Buffer.
const reader = await orders.reader("audit", 64);
for (;;) {
  const page = await reader.fetch();
  for (const m of page.records) {
    console.log(`${m.cursor} seq=${m.sequence} ${m.key} total=${m.value.total}`);
  }
  if (page.records.length === 0) break;
  await reader.ack();
}
await reader.close();

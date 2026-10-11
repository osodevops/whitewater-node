import { Cursor, WhitewaterClient } from "../dist/index.js";

/**
 * Ephemeral consumption: an anonymous temporary Reader with server-side
 * tail + wait — the "subscribe" pattern for live views, UIs, and REPLs.
 * Nothing is provisioned; the reader vanishes when the process exits.
 *
 * The loop:
 *
 *   temporaryFetch(tail, waitMs) → records → nextCursor → repeat
 *
 * wait_ms makes each call a long-poll: the Node holds the request until
 * records commit or the wait expires, then answers — so this loop does
 * NOT burn a request per poll interval; it sits inside the fetch.
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
await client.createFeed("demo.tail");

let after = /** @type {Cursor | null} */ (null); // null → feed tail position

while (true) {
  // Blocks up to wait_ms server-side; returns early when records land.
  const page = await client.feeds.temporaryFetch("demo.tail", {
    tail: true,
    waitMs: 2500,
    after,
    limit: 50,
  });

  for (const record of page.records) {
    console.log(
      "%s @ %s",
      record.payload.toString("utf8"),
      record.ingestTimeNs, // bigint — nanosecond precision, never number
    );
  }

  // Continue from the PAGE frontier, not the last record's cursor — the
  // page can be filtered or empty while the frontier still advanced.
  after = page.nextCursor;
}

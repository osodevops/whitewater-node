import { ReaderStart, WhitewaterClient } from "../dist/index.js";

/**
 * Durable consumption: a named Reader owns an independent cursor on the
 * Riverbed, so progress survives client restarts — the "subscribe"
 * pattern for applications, versus the ephemeral tail in tail-feed.mjs.
 *
 * The loop is explicit — Whitewater Readers pull rather than push:
 *
 *   open → [ fetch → process → ack ] → close
 *
 * ack() is the commit point: acknowledged progress is durable, so a
 * restarted client resumes after the last acked position. Process
 * records BEFORE acking them — ack-then-crash loses those records.
 */

const client = new WhitewaterClient({
  apiKey: "whitewater-local-development-admin-key",
  endpoints: [
    "http://localhost:7371",
    "http://localhost:7372",
    "http://localhost:7373",
  ],
});

// One-time provisioning: the Reader declaration is durable Control
// Plane state, not a per-session object.
await client.createDomain("demo");
await client.createFeed("demo.tail");
await client.createReader("demo-reader", "demo.tail", ReaderStart.beginning());

const reader = client.reader("demo-reader");

// open() fences this client as the session owner. Another client
// opening the same Reader supersedes this session (stale_epoch on the
// next fetch).
let session = await reader.open(64);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

try {
  for (;;) {
    // fetch() returns immediately with whatever is committed — a Reader
    // is not a long-poll. For "sit and wait" behavior, use
    // feeds.temporaryFetch({ tail, waitMs }) or pace empty fetches.
    const page = await reader.fetch(session, 50);

    for (const record of page.records) {
      console.log("%s @ %s", record.payload.toString("utf8"), record.ingestTimeNs);
    }

    // Commit durable progress AFTER processing the page. The page's
    // deliveredCursor covers every range the fetch drew from — ack it,
    // not the last record's cursor.
    if (page.deliveredCursor !== null) {
      session = await reader.ack(session, page.deliveredCursor);
    }

    if (page.records.length === 0) {
      await sleep(500); // poll cadence for live reads
    }
  }
} finally {
  await reader.close(session); // release the session fence
}

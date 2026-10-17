import { createHash } from "node:crypto";
import { codecs, WhitewaterClient } from "../dist/index.js";

/**
 * Permanent enrichment consumer — the Whitewater equivalent of a Kafka
 * consumer loop, with the plumbing removed:
 *
 *   for await (const msg of feed.consumer("enricher")) {
 *     await process(msg);
 *   }
 *
 *   // or: await feed.consumer("enricher").onRecord(process).run();
 *
 * feed.consumer(name) binds the durable named Reader (Kafka's
 * consumer-group equivalent): fetch -> deliver -> ack runs on a
 * supervised loop inside the SDK — transient failures retry with
 * backoff, a fenced session re-opens, and progress resumes from the
 * server-side cursor across restarts. No offset management, ever.
 *
 * Prerequisites (against the dev cluster): domain "users", feeds
 * users.update.request / users.update.enriched.request /
 * users.update.dlq, StateStore users.profiles with named key "user_id",
 * manual store users.stats.
 *
 * Replay honesty: between "enriched append committed" and "input acked"
 * a crash replays the input — so every effect carries a requestId
 * derived from the input messageId, which the server dedups. Replay
 * becomes a no-op instead of a duplicate. Poison pills are the app's
 * job: this example routes them to the DLQ.
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

// ---- surfaces, codec-bound once --------------------------------------

const inFeed = client.feed("users.update.request", codecs.json());
const outFeed = client.feed("users.update.enriched.request", codecs.json());
const dlqFeed = client.feed("users.update.dlq", codecs.json());

// users store: primary key email, declared named key "user_id".
const users = client.store("users.profiles", codecs.json());
// counters store: bucket key -> running total.
const counters = client.store("users.stats", codecs.json());

const outWriter = await outFeed.writer("enricher");
const dlqWriter = await dlqFeed.writer("enricher");

const deterministicId = (scope, messageId) =>
  createHash("sha256").update(`${scope}:${messageId}`).digest("hex")
    .slice(0, 32);

async function process(msg) {
  const req = msg.value;

  // 1. primary-key lookup by email — value only, no row unwrapping
  let profile = req.userEmail ? await users.find(req.userEmail) : null;

  // 2. named-key fallback — the declared "user_id" index on the store
  if (profile === null && req.userId) {
    profile = await users.findBy("user_id", req.userId);
  }

  // Idempotency key for every effect this input produces: a replay
  // after a crash re-sends the same requestId, which the server
  // dedups to the original append — no double-emit, ever.
  const effectId = deterministicId("enrich", msg.messageId);

  if (profile === null) {
    await dlqWriter.appendEvent({
      requestId: effectId,
      key: req.userEmail,
      value: {
        reason: "user not found",
        sourceFeed: "users.update.request",
        sourceMessageId: msg.messageId,
        sourceCursor: msg.cursor,
        original: req,
      },
      metadata: { reason: Buffer.from("enrichment-miss") },
    });
    return;
  }

  await outWriter.appendEvent({
    requestId: effectId,
    key: profile.user_id,
    value: {
      request_id: req.requestId,
      user_id: profile.user_id,
      email: profile.email,
      display_name: profile.display_name,
      region: profile.region,
      changes: req.changes,
    },
  });

  // 3. counters — bucket keys roll up day/week/month. RMW is safe here
  //    because this service is the only writer to these keys.
  const now = new Date();
  const day = now.toISOString().slice(0, 10);
  const weekStart = new Date(now);
  weekStart.setUTCDate(now.getUTCDate() - now.getUTCDay());
  const buckets = [
    `updates.day.${day}`,
    `updates.week.${weekStart.toISOString().slice(0, 10)}`,
    `updates.month.${day.slice(0, 7)}`,
  ];
  for (const bucket of buckets) {
    const count = await counters.getOrDefault(bucket, 0);
    await counters.put(bucket, count + 1);
  }
}

// The permanent loop: durable "enricher" Reader, supervised retries,
// per-record delivery, ack after each page's records are handled.
// SIGINT/SIGTERM -> close(): the in-flight page finishes, acks, and the
// session releases before the process exits.
const consumer = inFeed.consumer("enricher");
process.on("SIGINT", () => consumer.close());
process.on("SIGTERM", () => consumer.close());

for await (const msg of consumer) {
  await process(msg);
}

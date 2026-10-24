/**
 * `createFakeRiverbed` — the public unit-test double — exercised end to
 * end through the real client API: transport round-trips, error
 * mapping, and wire decoding all run for real.
 */
import { describe, expect, it, beforeEach, afterEach } from "vitest";
import {
  codecs,
  createFakeRiverbed,
  Conflict,
  type FakeRiverbed,
  ReaderStart,
  Refused,
  StaleSession,
  StateStore,
  WhitewaterClient,
} from "../src/index.js";

interface Order {
  id: string;
  total: number;
}

describe("FakeRiverbed unit-test double", () => {
  let riverbed: FakeRiverbed;
  let client: WhitewaterClient;

  beforeEach(async () => {
    riverbed = await createFakeRiverbed();
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: riverbed.endpoints,
    });
  });

  afterEach(async () => {
    await riverbed.close();
  });

  const orders = () => client.feed("orders.events", codecs.json<Order>());

  it("reports health through the client's compat check", async () => {
    expect(await client.healthy()).toBe(true);
  });

  it("append → fetch → ack commits progress; a reopened session stays drained", async () => {
    const feed = orders();
    const writer = await feed.writer("ingest");
    await writer.append("o1", { id: "o1", total: 7 });
    await writer.append("o2", { id: "o2", total: 11 });

    const reader = await feed.reader("audit", 64);
    const page = await reader.fetch();
    expect(page.records.map((m) => m.value.id)).toEqual(["o1", "o2"]);
    expect(page.deliveredCursor).not.toBeNull();
    await reader.ack();
    await reader.close();

    // A brand-new session resumes from the acknowledged frontier.
    const reopened = await feed.reader("audit", 64);
    const next = await reopened.fetch();
    expect(next.records).toHaveLength(0);
    await reopened.close();
  });

  it("redelivers an unacknowledged page after close and reopen", async () => {
    const feed = orders();
    const writer = await feed.writer("ingest");
    await writer.append("o1", { id: "o1", total: 7 });

    const first = await feed.reader("audit");
    expect((await first.fetch()).records).toHaveLength(1);
    await first.close(); // no ack — the delivered frontier dies here

    const second = await feed.reader("audit");
    const replay = await second.fetch();
    expect(replay.records.map((m) => m.value.id)).toEqual(["o1"]);
    await second.ack();
    await second.close();

    const third = await feed.reader("audit");
    expect((await third.fetch()).records).toHaveLength(0);
    await third.close();
  });

  it("dedupes identical requestId appends to one committed record", async () => {
    const feed = orders();
    const writer = await feed.writer("ingest");
    const first = await writer.appendEvent({
      requestId: "rid-1",
      key: "o1",
      value: { id: "o1", total: 7 },
    });
    const retry = await writer.appendEvent({
      requestId: "rid-1",
      key: "o1",
      value: { id: "o1", total: 7 },
    });
    expect(retry.deduplicated).toBe(true);
    expect(retry.messageId).toBe(first.messageId);
    expect(retry.cursor).toBe(first.cursor);

    const page = await feed.fetch({ limit: 8 });
    expect(page.records).toHaveLength(1); // committed once, not twice
  });

  it("refuses a stale writer session epoch with StaleSession", async () => {
    const feed = orders();
    const fenced = await feed.writer("ingest"); // session epoch 1
    await feed.writer("ingest"); // epoch 2 supersedes the first session
    const failure = await fenced.append("o1", { id: "o1", total: 7 }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(StaleSession);
    expect((failure as StaleSession).status).toBe(409);
    expect((failure as StaleSession).code).toBe("stale_epoch");
  });

  it("fences a superseded reader session on fetch", async () => {
    const feed = orders();
    const writer = await feed.writer("ingest");
    await writer.append("o1", { id: "o1", total: 7 });
    const stale = await feed.reader("audit"); // epoch 1
    await feed.reader("audit"); // epoch 2 fences epoch 1
    await expect(stale.fetch()).rejects.toBeInstanceOf(StaleSession);
  });

  it("round-trips a state store: put/get/getBy/iterate/delete/clear", async () => {
    const spec = StateStore.named("accounts.employees").addKey("department", false);
    const store = await client.stateStores.define(spec);
    await store.put("user-1", "profile-1", { department: "eng" });
    await store.put("user-2", "profile-2", { department: "ops" });

    const row = await store.get("user-1");
    expect(row).not.toBeNull();
    expect(row!.payload.toString("utf8")).toBe("profile-1");
    expect(row!.freshness?.behind).toBe(false);

    const group = await store.getBy("department", "eng");
    expect(group.rows.map((r) => r.key.toString("utf8"))).toEqual(["user-1"]);

    const keys: string[] = [];
    for await (const r of store.iterate()) keys.push(r.key.toString("utf8"));
    expect(keys).toEqual(["user-1", "user-2"]); // primary-key order

    await store.delete("user-1");
    expect(await store.get("user-1")).toBeNull();

    await store.clear();
    const rest: string[] = [];
    for await (const r of store.iterate()) rest.push(r.key.toString("utf8"));
    expect(rest).toEqual([]);
  });

  it("dedupes store mutations by mutation_id and conflicts on reuse", async () => {
    const mutationId = "00000000-0000-7000-8000-0000000000aa" as const;
    const store = client.stateStore("accounts.employees");
    const first = await store.put("user-1", "p1", undefined, mutationId);
    const retry = await store.put("user-1", "p1", undefined, mutationId);
    expect(retry.deduplicated).toBe(true);
    expect(retry.sequence).toBe(first.sequence);

    await expect(
      store.put("user-2", "p2", undefined, mutationId),
    ).rejects.toBeInstanceOf(Conflict);
  });

  it("serves ad-hoc appends and temporary fetches on the feed log", async () => {
    const feed = orders();
    await feed.append("ws-1", 1, 1, "o1", { id: "o1", total: 3 });
    const page = await feed.fetch({ limit: 4 });
    expect(page.records.map((m) => m.value.id)).toEqual(["o1"]);
    expect(page.nextCursor).not.toBeNull();

    const drained = await feed.fetch({ after: page.nextCursor });
    expect(drained.records).toHaveLength(0);
  });

  it("loads committed history through load()", async () => {
    const feed = orders();
    const writer = await feed.writer("ingest");
    await writer.append("o1", { id: "o1", total: 1 });
    await writer.append("o2", { id: "o2", total: 2 });
    const seen: string[] = [];
    const loaded = await feed.load(ReaderStart.beginning(), (messages) => {
      for (const m of messages) seen.push(m.value.id);
    });
    expect(loaded).toBe(2);
    expect(seen).toEqual(["o1", "o2"]);
  });

  it("answers an SDK-decodable 404 for unmodeled routes", async () => {
    const failure = await client.transport.post("/v1/subscriptions/members/join", {}).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(Refused);
    expect((failure as Refused).status).toBe(404);
  });
});

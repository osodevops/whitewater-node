import { describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { ReaderStart, RecordStore, WhitewaterClient } from "../src/index.js";

/**
 * Live store smoke against a running Riverbed — create Domain + Feed,
 * append keyed records with metadata, then `loadStore` a projected
 * history read and named-key lookups.
 *
 * Runs only when WHITEWATER_ENDPOINTS is set (comma-separated), e.g.
 * `http://localhost:7371,http://localhost:7372,http://localhost:7373`;
 * WHITEWATER_API_KEY defaults to the documented development key.
 */
const ENDPOINTS = process.env.WHITEWATER_ENDPOINTS;
const LIVE = Boolean(ENDPOINTS);

describe.skipIf(!LIVE)("live store", () => {
  it("creates, writes, loads, and queries a store", async () => {
    const client = new WhitewaterClient({
      apiKey: process.env.WHITEWATER_API_KEY ?? "whitewater-local-development-admin-key",
      endpoints: (ENDPOINTS as string).split(",").map((e) => e.trim()),
    });

    const domain = `smoke.node${(Date.now() & 0x7fffffff).toString(36)}`;
    const feed = `${domain}.events`;
    expect(await client.healthy()).toBe(true);
    await client.createDomain(domain);
    await client.createFeed(feed);

    const session = randomUUID();
    const rows: [string, string, string][] = [
      ["emp-1", "alice", "eng"],
      ["emp-2", "bob", "eng"],
      ["emp-3", "cara", "ops"],
    ];
    for (const [i, [key, payload, department]] of rows.entries()) {
      await client.feeds.append(feed, {
        requestId: randomUUID(),
        writerSessionId: session,
        writerEpoch: 1,
        sequence: i + 1,
        key: Buffer.from(key),
        payload: Buffer.from(payload),
        metadata: { department: Buffer.from(department) },
      });
    }

    const store = RecordStore.named("employee-store").addKey("department");
    const loaded = await client.loadStore(store).fromFeed(
      feed,
      ReaderStart.beginning(),
      (s, record) =>
        s
          .upsert(record)
          .withKey(record.key!)
          .withNamedKey("department", record.metadata["department"] ?? Buffer.alloc(0)),
    );

    expect(loaded).toBe(3);
    expect(store.size).toBe(3);
    expect(store.get("emp-1")?.payload.toString("utf8")).toBe("alice");
    expect(store.getBy("department", "eng").length).toBe(2);
    expect(store.getBy("department", "ops").length).toBe(1);

    const copy = RecordStore.named("employee-copy");
    store.copy(copy);
    expect(copy.getBy("department", "eng").length).toBe(2);
    expect(store.delete("emp-1")).toBe(true);
    expect(store.getBy("department", "eng").length).toBe(1);

    let visited = 0;
    await store.forEachParallel(2, async (entry) => {
      expect(entry.primaryKey.length).toBeGreaterThan(0);
      expect(entry.record.payload.length).toBeGreaterThan(0);
      visited += 1;
    });
    expect(visited).toBe(2);
  });
});

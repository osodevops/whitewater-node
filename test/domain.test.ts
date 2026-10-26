/**
 * `client.domain(name)` / `client.getDomain(name)` — the strongly-typed
 * Domain facade: scoped catalog views, typed Feed handles, and fluent
 * Reader/Writer builders bound to the Domain, exercised end to end
 * through the real client against `FakeRiverbed`.
 */
import { describe, expect, it, beforeEach, afterEach } from "vitest";
import {
  codecs,
  createFakeRiverbed,
  type FakeRiverbed,
  Feed,
  ReaderStart,
  Refused,
  WhitewaterClient,
} from "../src/index.js";

interface Order {
  id: string;
  total: number;
}

describe("typed Domain facade", () => {
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

  async function seeded() {
    await client.createDomain("shop");
    await client.createFeed("shop.orders");
    await client.createFeed("shop.returns");
    await client.createFeed("shop.orders.daily");
    await client.createFeed("other.events");
    return client.getDomain("shop");
  }

  it("composes Feed names lazily — no I/O until a catalog accessor", async () => {
    const domain = client.domain("unmade");
    const feed = domain.feed("orders");
    expect(feed.name).toBe("unmade.orders");
    await expect(domain.feedNames()).rejects.toBeInstanceOf(Refused);
  });

  it("getDomain materializes the catalog view", async () => {
    const domain = await seeded();
    expect(domain.name).toBe("shop");
    expect(await domain.domainId).toBeTruthy();
    expect(await domain.revision).toBeGreaterThan(0);
    expect((await domain.describe()).status).toBe("active");
  });

  it("getDomain refuses a missing Domain", async () => {
    await expect(client.getDomain("ghost")).rejects.toBeInstanceOf(Refused);
  });

  it("lists feeds scoped to the Domain", async () => {
    const domain = await seeded();
    expect(await domain.feedNames()).toEqual([
      "shop.orders",
      "shop.orders.daily",
      "shop.returns",
    ]);
  });

  it("prefix-filters feeds on the relative name", async () => {
    const domain = await seeded();
    expect((await domain.feeds("order")).map((f) => f.name)).toEqual([
      "shop.orders",
      "shop.orders.daily",
    ]);
    expect((await domain.feeds("returns")).map((f) => f.name)).toEqual(["shop.returns"]);
    expect((await domain.feeds()).map((f) => f.name)).toHaveLength(3);
  });

  it("refresh() re-pulls the snapshot", async () => {
    const domain = await seeded();
    expect(await domain.feedNames()).not.toContain("shop.payments");
    await client.createFeed("shop.payments");
    await domain.refresh();
    expect(await domain.feedNames()).toContain("shop.payments");
  });

  it("lists readers/writers/subscriptions scoped to the Domain", async () => {
    const domain = await seeded();
    await client.createWriter("orders.ingest", "shop.orders");
    await client.createReader("fulfillment", "shop.orders");
    await client.createSubscription("shop.subs.orders", "shop.orders");
    await client.createWriter("foreign.writer", "other.events");
    await domain.refresh();
    expect(await domain.writers()).toEqual(["orders.ingest"]);
    expect(await domain.readers()).toEqual(["fulfillment"]);
    expect(await domain.subscriptions()).toEqual(["shop.subs.orders"]);
  });

  it("composes relative and qualified feed names", () => {
    const domain = client.domain("shop");
    expect(domain.feed("orders").name).toBe("shop.orders");
    expect(domain.feed("shop.orders").name).toBe("shop.orders");
    expect(domain.getFeed("orders").name).toBe("shop.orders");
  });

  it("binds codecs on domain feeds", () => {
    const domain = client.domain("shop");
    const feed = domain.feed("orders", codecs.json<Order>());
    expect(feed).toBeInstanceOf(Feed);
  });

  it("layers a Feed's schema declaration over the Domain default", async () => {
    const shop = await seeded();
    await client.setDomainSchema("shop", {
      value: { format: "json", subject: "order-v1" },
    });
    await client.setFeedSchema("shop.orders", { key: { format: "text" } });
    await shop.refresh();               // snapshot cached the pre-schema catalog

    const domain = await shop.schema();
    expect(domain?.value?.format).toBe("json");
    expect(domain?.value?.subject).toBe("order-v1");

    const effective = await shop.feedSchema("shop.orders");
    expect(effective?.key?.format).toBe("text");
    expect(effective?.value?.format).toBe("json");       // inherited
    const declared = await shop.declaredFeedSchema("shop.orders");
    expect(declared?.key?.format).toBe("text");
    expect(declared?.value).toBeUndefined();

    const inherited = await shop.feedSchema("shop.returns");
    expect(inherited?.value?.format).toBe("json");
    expect(inherited?.key).toBeUndefined();
    expect(await shop.declaredFeedSchema("shop.returns")).toBeNull();
  });

  it("DROP SCHEMA reverts a Feed to Domain inheritance", async () => {
    const shop = await seeded();
    await client.setDomainSchema("shop", { value: { format: "json" } });
    await client.setFeedSchema("shop.orders", { value: { format: "avro" } });
    await shop.refresh();
    expect((await shop.feedSchema("shop.orders"))?.value?.format).toBe("avro");

    await client.clearFeedSchema("shop.orders");
    await shop.refresh();
    expect((await shop.feedSchema("shop.orders"))?.value?.format).toBe("json");
    expect(await shop.declaredFeedSchema("shop.orders")).toBeNull();
  });

  it("SET SCHEMA over WCL + SHOW SCHEMAS + DESCRIBE FEED agree", async () => {
    const shop = await seeded();
    await client.wcl(
      "SET SCHEMA ON DOMAIN shop VALUE json SUBJECT 'order-v1';" +
      " SET SCHEMA ON FEED shop.orders KEY protobuf DEFINITION 'syntax = proto3'",
    );
    await shop.refresh();
    const rows = (await client.wcl("SHOW SCHEMAS")).results[0].data as Array<{
      scope_kind: string;
      name: string;
      schema: Record<string, { format: string; subject?: string }>;
    }>;
    const byScope = new Map(rows.map((r) => [r.scope_kind + ":" + r.name, r.schema]));
    expect(byScope.get("domain:shop")?.value?.subject).toBe("order-v1");
    expect(byScope.get("feed:shop.orders")?.key?.format).toBe("protobuf");

    const described = (await client.commands([
      { command: "describe", kind: "feed", name: "shop.orders" },
    ])).results[0].data as {
      schema: { key: { format: string } };
      effective_schema: { value: { format: string } };
    };
    expect(described.schema.key.format).toBe("protobuf");
    expect(described.effective_schema.value.format).toBe("json");

    const effective = await shop.feedSchema("shop.orders");
    expect(effective?.key?.format).toBe("protobuf");
    expect(effective?.value?.format).toBe("json");
  });
});

describe("Domain.createFeed", () => {
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

  it("composes the Domain prefix and declares the feed", async () => {
    await client.createDomain("shop");
    const shop = await client.getDomain("shop");
    await shop.createFeed("orders");
    await shop.refresh();
    expect(shop.feed("orders").name).toBe("shop.orders");
    expect(await shop.feedNames()).toContain("shop.orders");
  });

  it("composes nested relative names inside the Domain", async () => {
    const shop = client.domain("shop");
    expect(shop.feed("x.y").name).toBe("shop.x.y");
  });
});

describe("Domain-scoped builders + forEach", () => {
  let riverbed: FakeRiverbed;
  let client: WhitewaterClient;

  beforeEach(async () => {
    riverbed = await createFakeRiverbed();
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: riverbed.endpoints,
    });
    await client.createDomain("shop");
  });

  afterEach(async () => {
    await riverbed.close();
  });

  it("createWriter/createReader resolve relative names inside the Domain", async () => {
    const shop = await client.getDomain("shop");
    await shop.createFeed("orders");
    const writer = await shop.createWriter("orders").writerId("ingest").open();
    await writer.append("order-1", Buffer.from("1"));

    const reader = await shop
      .createReader("orders")
      .readerId("fulfillment")
      .readerStartPoint(ReaderStart.BEGINNING)
      .open();
    const page = await reader.fetch();
    expect(page.records[0].key).toBe("order-1");
    await reader.close();
  });

  it("reader.forEach acks handled pages and replays the unacked tail", async () => {
    const shop = client.domain("shop");
    await shop.createFeed("orders");
    const writer = await shop.createWriter("orders").writerId("w").open();
    await writer.append("a", Buffer.from("1"));
    await writer.append("b", Buffer.from("2"));

    const reader = await shop
      .createReader("orders")
      .readerId("pump")
      .readerStartPoint(ReaderStart.BEGINNING)
      .capacity(1)
      .open();
    const seen: string[] = [];
    await reader.forEach((m) => {
      seen.push(m.key);
      return seen.length < 2;
    });
    expect(seen).toEqual(["a", "b"]);
    await reader.close();

    const again = await shop.createReader("orders").readerId("pump").open();
    expect((await again.fetch()).records[0].key).toBe("b");
    await again.close();
  });

  it("for await iterates records per page", async () => {
    const shop = client.domain("shop");
    await shop.createFeed("orders");
    const writer = await shop.createWriter("orders").writerId("w").open();
    await writer.append("a", Buffer.from("1"));
    const reader = await shop
      .createReader("orders")
      .readerId("iter")
      .readerStartPoint(ReaderStart.BEGINNING)
      .open();
    for await (const m of reader) {
      expect(m.key).toBe("a");
      break;
    }
    await reader.close();
  });
});

describe("ReaderBuilder", () => {
  let riverbed: FakeRiverbed;
  let client: WhitewaterClient;

  beforeEach(async () => {
    riverbed = await createFakeRiverbed();
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: riverbed.endpoints,
    });
    await client.createDomain("shop");
    await client.createFeed("shop.orders");
  });

  afterEach(async () => {
    await riverbed.close();
  });

  it("declares + opens a typed reader end to end", async () => {
    const shop = client.domain("shop");
    const writer = await client
      .createWriter(shop, "orders")
      .writerId("orders.ingest")
      .open();
    await writer.append("order-1", Buffer.from('{"total":9}'));

    const reader = await client
      .createReader(shop, shop.feed("orders"))
      .readerId("fulfillment")
      .readerStartPoint(ReaderStart.BEGINNING)
      .open();
    const page = await reader.fetch();
    expect(page.records.length).toBeGreaterThan(0);
    expect(page.records[0].key).toBe("order-1");
    await reader.close();
  });

  it("resolves string feed names relative to the Domain", () => {
    const shop = client.domain("shop");
    const builder = client.createReader(shop, "orders");
    expect(builder).toBeDefined();
  });

  it("requires readerId before open/declare", async () => {
    const shop = client.domain("shop");
    expect(() => client.createReader(shop, "orders").open()).toThrow(/readerId/);
    expect(() => client.createReader(shop, "orders").declare()).toThrow(
      /readerId/,
    );
  });

  it("refuses a Feed bound to another Domain — locally", () => {
    const other = client.domain("other");
    const foreign = other.feed("events");
    const shop = client.domain("shop");
    expect(() => client.createReader(shop, foreign)).toThrow(/outside Domain/);
  });

  it("applies readerStartPoint on first declaration", async () => {
    const shop = client.domain("shop");
    await client
      .createReader(shop, "orders")
      .readerId("r1")
      .readerStartPoint(ReaderStart.END)
      .declare();
    const described = await client.describeReader("r1");
    expect((described.results[0].data as { start: { kind: string } }).start.kind).toBe(
      "now",
    );
  });

  it("declare fails when the Reader already exists", async () => {
    const shop = client.domain("shop");
    await client.createReader(shop, "orders").readerId("r1").declare();
    await expect(
      client.createReader(shop, "orders").readerId("r1").declare(),
    ).rejects.toBeInstanceOf(Refused);
  });
});

describe("WriterBuilder", () => {
  let riverbed: FakeRiverbed;
  let client: WhitewaterClient;

  beforeEach(async () => {
    riverbed = await createFakeRiverbed();
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: riverbed.endpoints,
    });
    await client.createFeed("shop.orders");
  });

  afterEach(async () => {
    await riverbed.close();
  });

  it("declares + opens a typed writer end to end", async () => {
    const writer = await client
      .createWriter(client.domain("shop"), "orders")
      .writerId("orders.ingest")
      .open();
    await writer.append("k1", Buffer.from("payload"));
  });

  it("requires writerId before open/declare", async () => {
    const shop = client.domain("shop");
    expect(() => client.createWriter(shop, "orders").open()).toThrow(/writerId/);
    expect(() => client.createWriter(shop, "orders").declare()).toThrow(
      /writerId/,
    );
  });
});

describe("ReaderStart constants", () => {
  it("BEGINNING and END constants emit the right WCL", () => {
    expect(ReaderStart.wcl(ReaderStart.BEGINNING)).toBe("BEGINNING");
    expect(ReaderStart.wcl(ReaderStart.END)).toBe("NOW");
  });

  it("timestamp(Date) widens millis to nanos", () => {
    const start = ReaderStart.timestamp(new Date("2024-01-01T00:00:00Z"));
    expect(ReaderStart.wcl(start)).toBe("TIMESTAMP 1704067200000000000");
  });
});

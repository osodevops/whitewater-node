import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createFakeRiverbed,
  FakeRiverbed,
  SchemaTarget,
  SchemaType,
  SchemaValidation,
  StateStore,
  WhitewaterClient,
} from "../src/index.js";

describe("Whitewater Schema Registry (whitewater.schema.registry)", () => {
  let riverbed: FakeRiverbed;
  let client: WhitewaterClient;

  beforeEach(async () => {
    riverbed = await createFakeRiverbed();
    client = new WhitewaterClient({ apiKey: "test-key", endpoints: riverbed.endpoints });
  });

  afterEach(async () => {
    await riverbed.close();
  });

  it("rejects direct registration or access to whitewater.schema.registry", async () => {
    const ww = await client.createDomain("whitewater");
    expect(() => ww.stateStore("schema.registry")).toThrow();
    await expect(ww.defineStateStore(StateStore.named("schema.registry"))).rejects.toThrow();
  });

  it("validates Feed sync and async writes against associated key and message schemas", async () => {
    const orders = await client.createDomain("orders");
    await orders.createFeed("events");
    const feed = orders.feed("events");

    await client.updateSchema(
      "orders.events",
      JSON.stringify({ type: "string", pattern: "^ord-" }),
      SchemaTarget.Key,
      SchemaType.Json,
    );
    const entry = await client.updateSchema(
      "orders.events",
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
    );
    expect(entry.key).toBe("orders.events|message");
    expect(entry.version).toBe(1);

    const writer = await feed.writer("ingest");
    const ok = await writer.append("ord-1", JSON.stringify({ orderId: "ord-1", total: 25 }));
    expect(ok.messageId).toBeTruthy();

    await expect(
      writer.append("bad-key", JSON.stringify({ orderId: "ord-1", total: 25 })),
    ).rejects.toBeInstanceOf(SchemaValidation);

    await expect(
      writer.append("ord-2", JSON.stringify({ orderId: "ord-2", total: -5 })),
    ).rejects.toBeInstanceOf(SchemaValidation);

    // Async write() rejects the returned promise without breaking subsequent writes
    await expect(
      writer.write("ord-3", JSON.stringify({ orderId: "ord-3" })),
    ).rejects.toBeInstanceOf(SchemaValidation);

    const okAsync = await writer.write("ord-4", JSON.stringify({ orderId: "ord-4", total: 100 }));
    expect(okAsync.messageId).toBeTruthy();
    await writer.close();
  });

  it("validates StateStore put operations and enforces backward compatibility", async () => {
    const shop = await client.createDomain("shop");
    const store = await shop.defineStateStore(StateStore.named("customers"));

    await client.updateSchema(
      "shop.customers",
      'syntax = "proto3";\nmessage Customer {\n  string id = 1;\n  int32 tier = 2;\n}',
      SchemaTarget.Message,
      SchemaType.Protobuf,
    );

    await store.put("c1", JSON.stringify({ id: "c1", tier: 2 }));
    await expect(
      store.put("c2", JSON.stringify({ id: "c2", tier: "invalid" })),
    ).rejects.toBeInstanceOf(SchemaValidation);

    // Incompatible type change on field #2 is rejected
    await expect(
      client.updateSchema(
        "shop.customers",
        'syntax = "proto3";\nmessage Customer {\n  string id = 1;\n  string tier = 2;\n}',
        SchemaTarget.Message,
        SchemaType.Protobuf,
      ),
    ).rejects.toBeInstanceOf(SchemaValidation);

    // Adding an optional field #3 is compatible and bumps to v2
    const v2 = await client.updateSchema(
      "shop.customers",
      'syntax = "proto3";\nmessage Customer {\n  string id = 1;\n  int32 tier = 2;\n  string email = 3;\n}',
      SchemaTarget.Message,
      SchemaType.Protobuf,
    );
    expect(v2.version).toBe(2);
  });

  it("supports Avro schema validation and BACKWARD / FORWARD / FULL compatibility", async () => {
    const billing = await client.createDomain("billing");
    await billing.createFeed("invoices");
    const feed = billing.feed("invoices");

    const v1 = JSON.stringify({
      type: "record",
      name: "Invoice",
      fields: [
        { name: "id", type: "string" },
        { name: "amount", type: "long" },
      ],
    });
    await client.updateSchema("billing.invoices", v1, SchemaTarget.Message, SchemaType.Avro, "BACKWARD");

    // BACKWARD: adding required field without default fails
    const addReqNoDefault = JSON.stringify({
      type: "record",
      name: "Invoice",
      fields: [
        { name: "id", type: "string" },
        { name: "amount", type: "long" },
        { name: "currency", type: "string" },
      ],
    });
    expect(
      (await client.checkSchemaCompatibility("billing.invoices", addReqNoDefault, SchemaTarget.Message, SchemaType.Avro)).compatible,
    ).toBe(false);

    // FORWARD: adding a required field is forward-compatible, but dropping 'amount' fails
    await client.updateSchemaCompatibility("billing.invoices", SchemaTarget.Message, "FORWARD");
    expect(
      (await client.checkSchemaCompatibility("billing.invoices", addReqNoDefault, SchemaTarget.Message, SchemaType.Avro)).compatible,
    ).toBe(true);

    const dropAmount = JSON.stringify({
      type: "record",
      name: "Invoice",
      fields: [{ name: "id", type: "string" }],
    });
    expect(
      (await client.checkSchemaCompatibility("billing.invoices", dropAmount, SchemaTarget.Message, SchemaType.Avro)).compatible,
    ).toBe(false);

    // FULL: adding a field with default succeeds
    await client.updateSchemaCompatibility("billing.invoices", SchemaTarget.Message, "FULL");
    const goodFull = JSON.stringify({
      type: "record",
      name: "Invoice",
      fields: [
        { name: "id", type: "string" },
        { name: "amount", type: "long" },
        { name: "currency", type: "string", default: "EUR" },
      ],
    });
    const v2 = await client.updateSchema("billing.invoices", goodFull, SchemaTarget.Message, SchemaType.Avro, "FULL");
    expect(v2.version).toBe(2);

    const writer = await feed.writer("w");
    await writer.append("k1", JSON.stringify({ id: "inv-1", amount: 500 }));
    await expect(writer.append("k2", JSON.stringify({ id: "inv-2", amount: "bad" }))).rejects.toBeInstanceOf(SchemaValidation);
    await expect(writer.append("k3", JSON.stringify({ id: "inv-3", amount: 10, extra: 1 }))).rejects.toBeInstanceOf(SchemaValidation);
    await writer.close();
  });

  it("caches lookups and refreshes evolved schema version after TTL expires", async () => {
    const ops = await client.createDomain("ops");
    await ops.createFeed("metrics");

    const v1 = JSON.stringify({
      type: "object",
      required: ["host"],
      properties: { host: { type: "string" }, cpu: { type: "number" } },
      additionalProperties: false,
    });
    await client.updateSchema("ops.metrics", v1, SchemaTarget.Message, SchemaType.Json, "NONE");

    const producer = new WhitewaterClient({ apiKey: "test-key", endpoints: riverbed.endpoints });
    producer.schemas.setCacheTtl(40);
    const writer = await producer.domain("ops").feed("metrics").writer("p1");

    const before = producer.schemas.lookupCount;
    await writer.append("k1", JSON.stringify({ host: "n1" }));
    const afterFirst = producer.schemas.lookupCount;
    expect(afterFirst).toBe(before + 2);

    // Subsequent writes within TTL use cache with 0 remote lookups
    await writer.append("k2", JSON.stringify({ host: "n2" }));
    await writer.append("k3", JSON.stringify({ host: "n3" }));
    expect(producer.schemas.lookupCount).toBe(afterFirst);

    // Admin evolves schema to v2 requiring 'cpu'
    const v2 = JSON.stringify({
      type: "object",
      required: ["host", "cpu"],
      properties: { host: { type: "string" }, cpu: { type: "number" } },
      additionalProperties: false,
    });
    const evolved = await client.updateSchema("ops.metrics", v2, SchemaTarget.Message, SchemaType.Json, "NONE");
    expect(evolved.version).toBe(2);

    // Wait for 40ms TTL to expire
    await new Promise((r) => setTimeout(r, 60));

    // Producer now fetches v2 and rejects payload missing 'cpu'
    await expect(writer.append("k4", JSON.stringify({ host: "n4" }))).rejects.toBeInstanceOf(SchemaValidation);
    await writer.append("k4", JSON.stringify({ host: "n4", cpu: 0.75 }));
    await writer.close();
  });

  it("supports read-only linked targets and cross-cluster replication", async () => {
    const common = await client.createDomain("common");
    const billing = await client.createDomain("billing");
    await billing.createFeed("orders");

    const v1 = JSON.stringify({
      type: "object",
      required: ["orderId"],
      properties: { orderId: { type: "string" }, total: { type: "number" } },
      additionalProperties: false,
    });
    await client.updateSchema("common.order", v1, SchemaTarget.Message, SchemaType.Json, "NONE");

    const linked = await client.linkSchema("common.order", SchemaTarget.Message, "billing.orders", SchemaTarget.Message);
    expect(linked.readOnly).toBe(true);
    expect(linked.sourceKey).toBe("common.order|message");

    // Direct update on read-only linked target is blocked
    await expect(
      client.updateSchema("billing.orders", v1, SchemaTarget.Message, SchemaType.Json),
    ).rejects.toBeInstanceOf(SchemaValidation);

    // Evolving primary owner updates linked target automatically
    const v2 = JSON.stringify({
      type: "object",
      required: ["orderId", "total"],
      properties: { orderId: { type: "string" }, total: { type: "number" } },
      additionalProperties: false,
    });
    await client.updateSchema("common.order", v2, SchemaTarget.Message, SchemaType.Json, "NONE");
    const afterLink = await client.getSchema("billing.orders", SchemaTarget.Message);
    expect(afterLink?.version).toBe(2);

    // Replicate to a second cluster as a read-only replica
    const drRiverbed = await createFakeRiverbed();
    try {
      const drClient = new WhitewaterClient({ apiKey: "test-key", endpoints: drRiverbed.endpoints });
      const replica = await client.replicateSchemaTo(drClient, "prod-eu", "billing.orders", SchemaTarget.Message);
      expect(replica.readOnly).toBe(true);
      expect(replica.sourceCluster).toBe("prod-eu");
      expect(replica.version).toBe(2);
      await expect(
        drClient.updateSchema("billing.orders", v1, SchemaTarget.Message, SchemaType.Json),
      ).rejects.toBeInstanceOf(SchemaValidation);
    } finally {
      await drRiverbed.close();
    }
  });
});

import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import { Cursor, ReaderStart, WhitewaterClient } from "../src/index.js";

/**
 * The typed resource methods on WhitewaterClient are WCL builders: each must
 * serialize to exactly one statement on POST /v1/admin/wcl, and invalid names
 * must fail locally before any HTTP call.
 */
describe("WCL builders", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  let capturedPath = "";
  let capturedBody = "";

  beforeEach(async () => {
    capturedPath = "";
    capturedBody = "";
    server = http.createServer((req, res) => {
      capturedPath = req.url ?? "";
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        capturedBody = Buffer.concat(chunks).toString("utf8");
        res.setHeader("content-type", "application/json");
        res.end(
          JSON.stringify({
            request_id: "00000000-0000-0000-0000-000000000000",
            results: [],
          }),
        );
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: [`http://127.0.0.1:${port}`],
    });
  });

  afterEach(() => server.close());

  function script(): string {
    expect(capturedPath).toBe("/v1/admin/wcl");
    return (JSON.parse(capturedBody) as { script: string }).script;
  }

  it("creates", async () => {
    await client.createDomain("orders");
    expect(script()).toBe("CREATE DOMAIN orders");

    await client.createFeed("orders.events");
    expect(script()).toBe("CREATE FEED orders.events");

    await client.createWriter("checkout", "orders.events");
    expect(script()).toBe("CREATE WRITER checkout TO orders.events");

    await client.createRole("ops");
    expect(script()).toBe("CREATE ROLE ops");
  });

  it("readers and subscriptions", async () => {
    await client.createReader("audit", "orders.events");
    expect(script()).toBe("CREATE READER audit FROM orders.events START AT BEGINNING");

    await client.createReader("live", "orders.events", ReaderStart.now());
    expect(script()).toBe("CREATE READER live FROM orders.events START AT NOW");

    await client.createReader(
      "rewind",
      "orders.events",
      ReaderStart.timestampNs(1700000000000000000n),
    );
    expect(script()).toBe(
      "CREATE READER rewind FROM orders.events START AT TIMESTAMP 1700000000000000000",
    );

    await client.createReader(
      "resumed",
      "orders.events",
      ReaderStart.cursor(Cursor.of("abc'def")!),
    );
    expect(script()).toBe(
      "CREATE READER resumed FROM orders.events START AT CURSOR 'abc''def'",
    );

    await client.createSubscription("team", "orders.events", ReaderStart.now());
    expect(script()).toBe("CREATE SUBSCRIPTION team FROM orders.events START AT NOW");
  });

  it("sessions and seek", async () => {
    await client.openWriterSession("checkout");
    expect(script()).toBe("OPEN WRITER SESSION checkout");

    await client.seekReader("audit", ReaderStart.now());
    expect(script()).toBe("SEEK READER audit TO NOW");
  });

  it("describe, rename, drop, show", async () => {
    await client.describeFeed("orders.events");
    expect(script()).toBe("DESCRIBE FEED orders.events");

    await client.describeSubscription("team");
    expect(script()).toBe("DESCRIBE SUBSCRIPTION team");

    await client.renameDomain("orders", "billing");
    expect(script()).toBe("RENAME DOMAIN orders TO billing");

    await client.renameFeed("orders.events", "orders.accepted");
    expect(script()).toBe("RENAME FEED orders.events TO orders.accepted");

    await client.dropWriter("checkout");
    expect(script()).toBe("DROP WRITER checkout");

    await client.listFeeds();
    expect(script()).toBe("SHOW FEEDS");

    await client.listDomains();
    expect(script()).toBe("SHOW DOMAINS");
  });

  it("invalid names fail locally", () => {
    expect(() => client.createFeed("UPPER.case")).toThrow("invalid dotted name");
    expect(() => client.createFeed(".leading")).toThrow("invalid dotted name");
    expect(() => client.createDomain("two words")).toThrow("invalid dotted name");
    expect(() => client.createWriter("bad;name", "orders.events")).toThrow(
      "invalid resource name",
    );
    expect(() => client.createReader("a'b", "orders.events")).toThrow(
      "invalid resource name",
    );
    expect(() => client.renameDomain("orders", "INJECTION; DROP")).toThrow(
      "invalid dotted name",
    );
    expect(capturedPath).toBe("");
  });
});

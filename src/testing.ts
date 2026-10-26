/**
 * `FakeRiverbed` — an in-process Riverbed double for application unit
 * tests. `createFakeRiverbed()` serves real loopback HTTP on
 * `node:http` (no dependencies), so the full client stack — transport,
 * error mapping, wire decoding — runs exactly as it would against a
 * live Node:
 *
 * ```ts
 * const riverbed = await createFakeRiverbed();
 * const client = new WhitewaterClient({ apiKey: "test", endpoints: riverbed.endpoints });
 * // ...exercise application code through `client`...
 * await riverbed.close();
 * ```
 *
 * Modeled wire semantics (sdk/spec/PROTOCOL.md):
 *
 * - Writers: `OPEN WRITER SESSION` bumps a fencing epoch; appends carry
 *   (`writer`, `session_epoch`, `request_id`) and dedupe to the
 *   original `message_id`/`cursor` with `deduplicated: true`. A stale
 *   epoch answers `409 {code: "stale_epoch"}` — the SDK's
 *   {@link StaleSession}.
 * - Readers: `open`/`fetch`/`ack`/`close` with ordered pages over the
 *   feed's commit log. Fetch advances the delivered frontier; ack makes
 *   it durable; closing without ack means the next session redelivers
 *   from `acknowledged_cursor` (at-least-once).
 * - Temporary fetches: `after` continuation, `new_only`,
 *   `after_event_time_ns` seeks, and `tail` + `wait_ms` long-polls.
 * - StateStores: primary-key rows plus named-key postings; `put_row`/
 *   `delete_row`/`clear` dedupe by `mutation_id` — a reused id with
 *   different content conflicts (409).
 * - WCL/commands: `CREATE`/`DROP`/`RENAME` for feeds, writers, readers,
 *   and subscriptions mutate the catalog; `OPEN WRITER SESSION` and
 *   `SEEK READER` behave; everything else is a canned `ok`.
 * - `event_time_ns`/`ingest_time_ns` travel as decimal strings — never
 *   imprecise JS numbers; cursors are opaque zero-padded positions.
 *
 * Deliberate leniency (a double, not a conformance oracle): resources
 * referenced before declaration are auto-provisioned — an undeclared
 * Writer binds the sole declared feed (or a lazily-created `default`
 * feed; ambiguous multi-feed use is refused), and stores come into
 * being on first use with named keys auto-declared on `put`.
 * Unmodeled routes — subscription member leases, index queries,
 * sync/rebuild — answer `404 {code: "not_found"}`, which the SDK
 * decodes as {@link Refused}.
 */

import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { URL, type URLSearchParams } from "node:url";

type Json = Record<string, unknown>;

const NODE_ID = "fake-node-1";
const FEEDBACK = {
  recommended_batch_count: 64,
  recommended_batch_bytes: 1_048_576,
  pressure: 0,
  retry_after_ms: 0,
  max_frame_bytes: 4_194_304,
  hot_key: false,
};
const LIMITS = {
  max_frame_bytes: 4_194_304,
  max_committed_read_bytes: 16_777_216,
  max_logical_read_frames: 1_024,
  max_logical_read_ranges: 16,
  subscription_member_fetch_max_limit: 512,
  subscription_member_max_lease_ticks: 600,
};

const asObj = (v: unknown): Json =>
  v !== null && typeof v === "object" && !Array.isArray(v) ? (v as Json) : {};
const asList = (v: unknown): unknown[] => (Array.isArray(v) ? v : []);
const str = (v: unknown): string => (v == null ? "" : String(v));
const int = (v: unknown, fallback: number): number => {
  const n = Number(v);
  return Number.isSafeInteger(n) && n >= 0 ? n : fallback;
};

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | null,
    message: string,
  ) {
    super(message);
  }
}

function refuse(status: number, message: string, code: string | null = null): never {
  throw new HttpError(status, code, message);
}

/** One committed record in a Feed's log; cursors derive from position. */
interface StoredRecord {
  position: number;
  cursor: string;
  messageId: string;
  producerId: string;
  sequence: number;
  eventTimeNs: bigint;
  ingestTimeNs: bigint;
  keyBase64: string | null;
  payloadBase64: string;
  metadataBase64: Record<string, string>;
}

class FakeFeed {
  readonly id = randomUUID();
  readonly createdAtNs = nowNs();
  readonly log: StoredRecord[] = [];
  private readonly waiters = new Set<() => void>();

  constructor(public name: string) {}

  /** Opaque cursor for a 1-based log position — the fake owns the
   * format; SDKs must treat it as pass-through. */
  cursorAt(position: number): string {
    return String(position).padStart(20, "0");
  }

  commit(
    fields: Omit<StoredRecord, "position" | "cursor" | "ingestTimeNs" | "eventTimeNs"> & {
      eventTimeNs: bigint | null;
    },
  ): StoredRecord {
    const position = this.log.length + 1;
    const ingestTimeNs = nowNs();
    const record: StoredRecord = {
      ...fields,
      position,
      cursor: this.cursorAt(position),
      ingestTimeNs,
      eventTimeNs: fields.eventTimeNs ?? ingestTimeNs,
    };
    this.log.push(record);
    this.wake();
    return record;
  }

  /** Release every parked tail fetch. */
  wake(): void {
    for (const wake of this.waiters) wake();
    this.waiters.clear();
  }

  /** Resolve once a record commits or `waitMs` elapses — tail long-poll. */
  waitForAppend(waitMs: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        this.waiters.delete(wake);
        resolve();
      }, waitMs);
      this.waiters.add(wake);
    });
  }
}

class FakeWriter {
  readonly id = randomUUID();
  readonly createdAtNs = nowNs();
  /** Current fencing epoch; 0 until the first session opens. */
  epoch = 0;
  sequence = 0;
  /** `${session_epoch}:${request_id}` → the committed record. */
  readonly dedup = new Map<string, StoredRecord>();

  constructor(
    readonly name: string,
    public feed: string,
  ) {}
}

class FakeReader {
  readonly id = randomUUID();
  readonly createdAtNs = nowNs();
  sessionEpoch = 0;
  capacity = 64;
  open = false;
  /** The declared START AT kind — beginning | now | cursor | timestamp. */
  startKind = "beginning";
  /** Durable restart point — survives close/reopen. */
  acknowledged = 0;
  /** Session delivery frontier — resets to `acknowledged` on open. */
  delivered = 0;
  /** `request_id` → recorded body + response (identical retries replay). */
  readonly dedup = new Map<string, { body: string; response: Json }>();

  constructor(
    readonly name: string,
    public feed: string,
  ) {}
}

interface StoredRow {
  keyBase64: string;
  payloadBase64: string;
  /** named key → wire `values` array (typed {kind, value|value_base64}). */
  named: Record<string, unknown[]>;
  appliedSeq: number;
}

class FakeStore {
  readonly id = randomUUID();
  readonly createdAtNs = nowNs();
  sequence = 0;
  readonly declaredKeys = new Set<string>();
  /** key_base64 → row. */
  readonly rows = new Map<string, StoredRow>();
  /** mutation_id → request hash + committed result. */
  readonly mutations = new Map<string, { hash: string; result: Json }>();

  constructor(readonly name: string) {}
}

export interface FakeRiverbedOptions {
  /** Bind host; defaults to loopback. */
  host?: string;
  /** Bind port; defaults to 0 (an ephemeral free port). */
  port?: number;
}

/**
 * A running loopback Riverbed double. Use {@link endpoints} to point a
 * {@link WhitewaterClient} at it and {@link close} to tear it down.
 */
export class FakeRiverbed {
  private readonly server: Server;
  private readonly sockets = new Set<Socket>();
  private readonly feeds = new Map<string, FakeFeed>();
  private readonly writers = new Map<string, FakeWriter>();
  private readonly readers = new Map<string, FakeReader>();
  private readonly subscriptions = new Map<
    string,
    { id: string; feed: string; startKind: string; createdAtNs: bigint }
  >();
  private readonly stores = new Map<string, FakeStore>();
  private readonly pipes = new Map<string, Json>();
  private readonly domains = new Map<string, { id: string; createdAtNs: bigint }>();
  private readonly schemas = new Map<string, Json>();
  private readonly roles = new Set<string>();
  private readonly roleIds = new Map<string, string>();
  /** `${feed}:${writer_session_id}` → last committed sequence (raw path). */
  private readonly feedSequences = new Map<string, number>();
  /** `${feed}:${writer_session_id}:${request_id}` → committed record. */
  private readonly feedDedup = new Map<string, StoredRecord>();
  private revision = 0;
  private closed = false;

  /** The endpoint URLs this fake serves — feed to `WhitewaterClient`. */
  readonly endpoints: string[];

  private constructor(server: Server, port: number) {
    this.server = server;
    this.endpoints = [`http://127.0.0.1:${port}`];
  }

  /** The single Node endpoint — convenience over `endpoints[0]`. */
  get endpoint(): string {
    return this.endpoints[0];
  }

  /** Stop serving and release the port (sockets are destroyed so
   * keep-alive connections cannot wedge shutdown). */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const feed of this.feeds.values()) feed.wake();
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }

  /** Bind an ephemeral port and begin serving. */
  static async start(options: FakeRiverbedOptions = {}): Promise<FakeRiverbed> {
    const fake = new FakeRiverbed(createServer(), 0);
    fake.server.on("connection", (socket: Socket) => {
      fake.sockets.add(socket);
      socket.on("close", () => fake.sockets.delete(socket));
    });
    fake.server.on("request", (req, res) => void fake.serve(req, res));
    const host = options.host ?? "127.0.0.1";
    await new Promise<void>((resolve, reject) => {
      fake.server.once("error", reject);
      fake.server.listen(options.port ?? 0, host, resolve);
    });
    const { port } = fake.server.address() as AddressInfo;
    fake.endpoints[0] = `http://${host}:${port}`;
    return fake;
  }

  // ---- HTTP plumbing ---------------------------------------------------------

  private async serve(req: IncomingMessage, res: ServerResponse): Promise<void> {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      const body = req.method === "GET" ? {} : asObj(await readJson(req));
      const result = await this.route(req.method ?? "GET", url.pathname, url.searchParams, body);
      reply(res, 200, result);
    } catch (error) {
      if (error instanceof HttpError) {
        reply(res, error.status, {
          error: error.message,
          ...(error.code !== null ? { code: error.code } : {}),
        });
      } else {
        reply(res, 500, {
          error: `fake riverbed: ${(error as Error).message}`,
          code: "internal",
        });
      }
    }
  }

  private route(
    method: string,
    path: string,
    query: URLSearchParams,
    body: Json,
  ): Json | Json[] | Promise<Json | Json[]> {
    if (method === "GET") {
      if (path === "/health") return this.health();
      if (path === "/v1/feeds/records") return this.feedRecords(query);
      if (path === "/v1/node/metrics") return { node_id: NODE_ID, status: "ok" };
      if (path === "/v1/cluster/members") {
        return { members: [{ node_id: NODE_ID, endpoints: this.endpoints }] };
      }
      const demand = /^\/v1\/admin\/demand\/(\S+)$/.exec(path);
      if (demand) return this.demand(demand[1]);
      if (path === "/v1/admin/subscriptions/progress") {
        return { subscription: str(query.get("subscription")), members: [] };
      }
    } else if (method === "POST") {
      switch (path) {
        case "/v1/admin/wcl":
          return this.wcl(body);
        case "/v1/admin/commands":
          return this.commands(body);
        case "/v1/writers/append":
          return this.writerAppend(asObj(body));
        case "/v1/writers/append-batch":
          return this.writerAppendBatch(asObj(body));
        case "/v1/readers/open":
          return this.readerOpen(body);
        case "/v1/readers/fetch":
          return this.readerFetch(body);
        case "/v1/readers/ack":
          return this.readerAck(body);
        case "/v1/readers/close":
          return this.readerClose(body);
        case "/v1/readers/temporary/fetch":
          return this.temporaryFetch(body);
        case "/v1/feeds/append":
          return this.feedAppend(body);
        case "/v1/state-stores/mutate":
          return this.storeMutate(body);
        case "/v1/state-stores/lookup":
          return this.storeLookup(body);
        case "/v1/state-stores/get-by":
          return this.storeGetBy(body);
        case "/v1/state-stores/iterate":
          return this.storeIterate(body);
        case "/v1/state-stores/clear":
          return this.storeClear(body);
      }
    }
    return refuse(404, `no route: ${method} ${path}`, "not_found");
  }

  // ---- Catalog ---------------------------------------------------------------

  private domain(name: string): { id: string; createdAtNs: bigint } {
    let domain = this.domains.get(name);
    if (!domain) {
      domain = { id: randomUUID(), createdAtNs: nowNs() };
      this.domains.set(name, domain);
    }
    return domain;
  }

  /** The `space_id` a resource name falls under — the fake scopes by
   * first dotted segment, matching its domain derivation. */
  private spaceIdOf(resourceName: string): string | null {
    return this.domains.get(resourceName.split(".")[0])?.id ?? null;
  }

  private feed(name: string): FakeFeed {
    let feed = this.feeds.get(name);
    if (!feed) {
      feed = new FakeFeed(name);
      this.feeds.set(name, feed);
      // Declarations materialize their owning Domain on first
      // reference — the typed facade never declares it itself.
      this.domain(name.split(".")[0]);
    }
    return feed;
  }

  /** The feed an undeclared Writer/Reader binds: the sole declared
   * feed, or a lazily-created `default`. Ambiguous when several exist. */
  private implicitFeed(): FakeFeed {
    if (this.feeds.size === 0) return this.feed("default");
    if (this.feeds.size === 1) return this.feeds.values().next().value!;
    refuse(400, "undeclared writer/reader is ambiguous across multiple feeds");
  }

  /** Resolve a cursor onto a log position; unreadable cursors refuse. */
  private positionOf(feed: FakeFeed, cursor: string): number {
    if (!/^\d+$/.test(cursor)) refuse(400, `unreadable cursor: ${cursor}`);
    return Math.min(parseInt(cursor, 10), feed.log.length);
  }

  private store(name: string): FakeStore {
    let store = this.stores.get(name);
    if (!store) {
      store = new FakeStore(name);
      this.stores.set(name, store);
    }
    return store;
  }

  // ---- Admin: WCL + typed commands --------------------------------------------

  private health(): Json {
    return {
      status: "ok",
      node_id: NODE_ID,
      members: 1,
      protocol: { major: 1, minor: 0 },
      limits: { ...LIMITS },
    };
  }

  private demand(kind: string): Json {
    switch (kind) {
      case "feeds":
        return {
          feeds: [...this.feeds.values()].map((f) => ({
            feed: f.name,
            appended_records: f.log.length,
          })),
          domains: {},
        };
      case "readers":
        return {
          readers: [...this.readers.values()].map((r) => ({ reader: r.name })),
          domains: {},
        };
      case "subscriptions":
        return {
          subscriptions: [...this.subscriptions.keys()].map((s) => ({ subscription: s })),
          domains: {},
        };
      case "indexes":
        return { indexes: [], domains: {} };
      case "pipes":
        return { pipes: [...this.pipes.values()].map((p) => ({ pipe: p.name })), domains: {} };
      case "hot-keys":
        return { hot_keys: [] };
      default:
        return refuse(404, `no route: /v1/admin/demand/${kind}`, "not_found");
    }
  }

  private execution(requestId: string | undefined, results: Json[]): Json {
    this.revision += 1;
    return {
      request_id: requestId || randomUUID(),
      revision: this.revision,
      authority: "control_plane",
      warning: "",
      results,
    };
  }

  private wcl(body: Json): Json {
    const script = str(body.script);
    const statements = script
      .split(";")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    const results = statements.map((s) => this.statement(s));
    return this.execution(
      str(body.request_id) || undefined,
      results.length > 0 ? results : [{ statement: "", message: "ok", data: {} }],
    );
  }

  private statement(text: string): Json {
    const ok = (data: Json = {}): Json => ({ statement: text, message: "ok", data });
    let m: RegExpExecArray | null;

    if ((m = /^CREATE\s+WRITER\s+(\S+)\s+TO\s+(\S+)$/i.exec(text))) {
      const [, name, feedName] = m;
      if (this.writers.has(name)) refuse(400, `writer '${name}' already exists`);
      this.writers.set(name, new FakeWriter(name, this.feed(feedName).name));
      return ok();
    }
    if ((m = /^OPEN\s+WRITER\s+SESSION\s+(\S+)$/i.exec(text))) {
      const writer = this.writers.get(m[1]);
      if (!writer) refuse(400, `unknown writer '${m[1]}'`);
      writer.epoch += 1;
      return ok({ session_epoch: writer.epoch, writer_id: writer.id });
    }
    if ((m = /^CREATE\s+READER\s+(\S+)\s+FROM\s+(\S+)(?:\s+START\s+AT\s+(.+))?$/i.exec(text))) {
      const [, name, feedName, start] = m;
      if (this.readers.has(name)) refuse(400, `reader '${name}' already exists`);
      const feed = this.feed(feedName);
      const reader = new FakeReader(name, feed.name);
      reader.startKind = this.startKind(start);
      reader.acknowledged = reader.delivered = this.startPosition(feed, start);
      this.readers.set(name, reader);
      return ok({ reader_id: reader.id, feed_id: feed.id });
    }
    if ((m = /^SEEK\s+READER\s+(\S+)\s+TO\s+(.+)$/i.exec(text))) {
      const reader = this.readers.get(m[1]);
      if (!reader) refuse(400, `unknown reader '${m[1]}'`);
      const position = this.startPosition(this.feed(reader.feed), m[2]);
      reader.acknowledged = reader.delivered = position;
      return ok();
    }
    if ((m = /^CREATE\s+FEED\s+(\S+)$/i.exec(text))) {
      if (this.feeds.has(m[1])) refuse(400, `feed '${m[1]}' already exists`);
      this.feed(m[1]);
      return ok({ feed_id: this.feed(m[1]).id });
    }
    if (
      (m = /^CREATE\s+SUBSCRIPTION\s+(\S+)\s+FROM\s+(\S+)(?:\s+START\s+AT\s+(.+))?$/i.exec(text))
    ) {
      if (this.subscriptions.has(m[1])) {
        refuse(400, `subscription '${m[1]}' already exists`);
      }
      this.subscriptions.set(m[1], {
        id: randomUUID(),
        feed: this.feed(m[2]).name,
        startKind: this.startKind(m[3]),
        createdAtNs: nowNs(),
      });
      return ok();
    }
    if ((m = /^CREATE\s+DOMAIN\s+(\S+)$/i.exec(text))) {
      if (this.domains.has(m[1])) refuse(400, `domain '${m[1]}' already exists`);
      this.domain(m[1]);
      return ok(this.resourceRow("domains", this.domains.get(m[1])!, m[1]));
    }
    if ((m = /^DESCRIBE\s+(\S+)\s+(\S+)$/i.exec(text))) {
      return ok(this.describe(m[1].toUpperCase(), m[2]));
    }
    if ((m = /^CREATE\s+ROLE\s+(\S+)$/i.exec(text))) {
      if (this.roles.has(m[1])) refuse(400, `role '${m[1]}' already exists`);
      this.roles.add(m[1]);
      this.roleIds.set(m[1], randomUUID());
      return ok();
    }
    if (
      (m = /^SET\s+SCHEMA\s+ON\s+(DOMAIN|FEED)\s+(\S+)\s+(.+)$/i.exec(text))
    ) {
      // SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <fmt> [SUBJECT 's'] [DEFINITION 'd'])+
      const key = m[1].toLowerCase() + ":" + m[2];
      const schema: Json = { ...(this.schemas.get(key) ?? {}) };
      const clause = /(KEY|VALUE)\s+(\S+)((?:\s+(?:SUBJECT|DEFINITION)\s+'(?:[^']|'')*')*)/gi;
      let side;
      while ((side = clause.exec(m[3])) !== null) {
        const decl: Json = { format: side[2].toLowerCase() };
        const attrs = /(SUBJECT|DEFINITION)\s+'((?:[^']|'')*)'/gi;
        let attr;
        while ((attr = attrs.exec(side[3])) !== null) {
          decl[attr[1].toLowerCase()] = attr[2].replace(/''/g, "'");
        }
        schema[side[1].toLowerCase()] = decl;
      }
      this.schemas.set(key, schema);
      return ok();
    }
    if (
      (m = /^DROP\s+SCHEMA\s+ON\s+(DOMAIN|FEED)\s+(\S+)$/i.exec(text))
    ) {
      this.schemas.delete(m[1].toLowerCase() + ":" + m[2]);
      return ok();
    }
    if ((m = /^DROP\s+(FEED|WRITER|READER|SUBSCRIPTION|DOMAIN|ROLE)\s+(\S+)$/i.exec(text))) {
      this.drop(m[1].toUpperCase(), m[2]);
      return ok();
    }
    if (
      (m = /^RENAME\s+(FEED|WRITER|READER|SUBSCRIPTION|DOMAIN|ROLE)\s+(\S+)\s+TO\s+(\S+)$/i.exec(
        text,
      ))
    ) {
      this.rename(m[1].toUpperCase(), m[2], m[3]);
      return ok();
    }
    if ((m = /^SHOW\s+(\S+)$/i.exec(text))) {
      return ok(this.show(m[1].toUpperCase()) as unknown as Json);
    }
    return ok(); // GRANT/… — canned success
  }

  /** `START AT` operand → its declared kind. */
  private startKind(start: string | undefined): string {
    const s = (start ?? "").trim().toUpperCase();
    if (s.startsWith("NOW")) return "now";
    if (s.startsWith("CURSOR")) return "cursor";
    if (s.startsWith("TIMESTAMP")) return "timestamp";
    return "beginning";
  }

  /** The catalog row SHOW/DESCRIBE surfaces for one resource — field
   * names match the server's `space_id`/`feed_id` join keys. */
  private resourceRow(kind: string, item: unknown, name?: string): Json {
    switch (kind) {
      case "domains": {
        const d = item as { id: string; createdAtNs: bigint };
        return {
          name: name ?? "",
          space_id: d.id,
          domain_id: d.id,
          status: "active",
          created_at_ns: d.createdAtNs.toString(10),
          schema: this.schemas.get("domain:" + (name ?? "")) ?? null,
        };
      }
      case "feeds": {
        const f = item as FakeFeed;
        return {
          name: f.name,
          feed_id: f.id,
          space_id: this.spaceIdOf(f.name),
          status: "active",
          created_at_ns: f.createdAtNs.toString(10),
          schema: this.schemas.get("feed:" + f.name) ?? null,
          effective_schema: this.effectiveSchema(f.name),
        };
      }
      case "writers": {
        const w = item as FakeWriter;
        return {
          name: w.name,
          writer_id: w.id,
          feed: w.feed,
          feed_id: this.feeds.get(w.feed)?.id ?? null,
          status: "active",
          created_at_ns: w.createdAtNs.toString(10),
          session_epoch: w.epoch,
          session_active: w.epoch > 0,
        };
      }
      case "readers": {
        const r = item as FakeReader;
        return {
          name: r.name,
          reader_id: r.id,
          feed: r.feed,
          feed_id: this.feeds.get(r.feed)?.id ?? null,
          start: { kind: r.startKind },
          status: "active",
          created_at_ns: r.createdAtNs.toString(10),
          session_epoch: r.sessionEpoch,
          session_active: r.open,
          session_capacity: r.capacity,
          acknowledged_cursor: this.feeds.get(r.feed)?.cursorAt(r.acknowledged) ?? null,
        };
      }
      case "subscriptions": {
        const s = item as { id: string; feed: string; startKind: string; createdAtNs: bigint };
        return {
          name: name ?? "",
          subscription_id: s.id,
          space_id: this.spaceIdOf(s.feed),
          feed: s.feed,
          start: { kind: s.startKind },
          stage: "declared",
          status: "active",
          created_at_ns: s.createdAtNs.toString(10),
        };
      }
      case "stores": {
        const s = item as FakeStore;
        return {
          name: s.name,
          store_id: s.id,
          space_id: this.spaceIdOf(s.name),
          status: "active",
          created_at_ns: s.createdAtNs.toString(10),
        };
      }
      case "pipes": {
        const pipe = item as Json;
        const sub = [...this.subscriptions.values()].find(
          (s) => s.id === pipe.subscription_id,
        );
        return {
          ...pipe,
          pipe_id: pipe.pipe_id ?? null,
          space_id: sub != null ? this.spaceIdOf(sub.feed) : null,
        };
      }
      case "roles": {
        const roleName = String(item);
        return {
          name: roleName,
          role_id: this.roleIds.get(roleName) ?? "",
          status: "active",
        };
      }
      default:
        return { name: name ?? String(item) };
    }
  }

  /** The feed's declared schema layered over its Domain's — the
   * feed's sides win; missing sides inherit. */
  private effectiveSchema(feedName: string): Json | null {
    const own = this.schemas.get("feed:" + feedName);
    const domain = feedName.split(".")[0];
    const inherited = this.schemas.get("domain:" + domain);
    if (own === undefined) return inherited ?? null;
    if (inherited === undefined) return own;
    return { ...inherited, ...own };
  }

  /** `DESCRIBE <kind> <name>` — the row, or a refusal when missing. */
  private describe(kind: string, name: string): Json {
    switch (kind) {
      case "DOMAIN":
      case "SPACE": {
        const d = this.domains.get(name);
        if (!d) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("domains", d, name);
      }
      case "FEED": {
        const f = this.feeds.get(name);
        if (!f) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("feeds", f);
      }
      case "WRITER": {
        const w = this.writers.get(name);
        if (!w) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("writers", w);
      }
      case "READER": {
        const r = this.readers.get(name);
        if (!r) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("readers", r);
      }
      case "SUBSCRIPTION": {
        const s = this.subscriptions.get(name);
        if (!s) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("subscriptions", s, name);
      }
      case "STATESTORE":
      case "STATE_STORE": {
        const s = this.stores.get(name);
        if (!s) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("stores", s);
      }
      case "PIPE": {
        const p = this.pipes.get(name);
        if (!p) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("pipes", p);
      }
      case "ROLE": {
        if (!this.roles.has(name)) refuse(400, `resource not found: ${name}`);
        return this.resourceRow("roles", name);
      }
      default:
        refuse(400, `unsupported DESCRIBE resource ${kind}`);
    }
  }

  private drop(kind: string, name: string): void {
    switch (kind) {
      case "FEED":
        this.feeds.delete(name);
        for (const w of this.writers.values()) if (w.feed === name) w.feed = "default";
        for (const r of this.readers.values()) if (r.feed === name) r.feed = "default";
        break;
      case "WRITER":
        this.writers.delete(name);
        break;
      case "READER":
        this.readers.delete(name);
        break;
      case "SUBSCRIPTION":
        this.subscriptions.delete(name);
        break;
      case "DOMAIN":
        this.domains.delete(name);
        break;
      case "ROLE":
        this.roles.delete(name);
        this.roleIds.delete(name);
        break;
    }
  }

  private rename(kind: string, current: string, renamed: string): void {
    switch (kind) {
      case "FEED": {
        const feed = this.feeds.get(current);
        if (feed) {
          this.feeds.delete(current);
          feed.name = renamed;
          this.feeds.set(renamed, feed);
          for (const w of this.writers.values()) if (w.feed === current) w.feed = renamed;
          for (const r of this.readers.values()) if (r.feed === current) r.feed = renamed;
        }
        break;
      }
      case "WRITER": {
        const writer = this.writers.get(current);
        if (writer) {
          this.writers.delete(current);
          this.writers.set(renamed, new FakeWriter(renamed, writer.feed));
        }
        break;
      }
      case "READER": {
        const reader = this.readers.get(current);
        if (reader) {
          this.readers.delete(current);
          this.readers.set(renamed, reader);
        }
        break;
      }
      case "SUBSCRIPTION": {
        const sub = this.subscriptions.get(current);
        if (sub) {
          this.subscriptions.delete(current);
          this.subscriptions.set(renamed, sub);
        }
        break;
      }
      case "DOMAIN": {
        const domain = this.domains.get(current);
        if (domain) {
          this.domains.delete(current);
          this.domains.set(renamed, domain);
        }
        break;
      }
      case "ROLE": {
        if (this.roles.delete(current)) {
          this.roles.add(renamed);
          const id = this.roleIds.get(current);
          if (id) {
            this.roleIds.delete(current);
            this.roleIds.set(renamed, id);
          }
        }
        break;
      }
    }
  }

  private show(kind: string): Json[] {
    switch (kind) {
      case "FEEDS":
        return [...this.feeds.values()].map((f) => this.resourceRow("feeds", f));
      case "WRITERS":
        return [...this.writers.values()].map((w) => this.resourceRow("writers", w));
      case "READERS":
        return [...this.readers.values()].map((r) => this.resourceRow("readers", r));
      case "SUBSCRIPTIONS":
        return [...this.subscriptions.entries()].map(([name, s]) =>
          this.resourceRow("subscriptions", s, name),
        );
      case "DOMAINS":
      case "SPACES":
        return [...this.domains.entries()].map(([name, d]) =>
          this.resourceRow("domains", d, name),
        );
      case "STATESTORES":
      case "STATE_STORES":
        return [...this.stores.values()].map((s) => this.resourceRow("stores", s));
      case "ROLES":
        return [...this.roles].map((name) => this.resourceRow("roles", name));
      case "GRANTS":
        return [];
      case "PIPES":
        return [...this.pipes.values()].map((p) => this.resourceRow("pipes", p));
      case "SCHEMAS":
        return [...this.schemas.entries()].map(([key, schema]) => ({
          scope_kind: key.split(":")[0],
          name: key.split(":").slice(1).join(":"),
          schema,
        }));
      default:
        return [];
    }
  }

  /** `START AT` operand → the log position a Reader resumes from. */
  private startPosition(feed: FakeFeed, start: string | undefined): number {
    if (start === undefined) return 0;
    const s = start.trim();
    if (/^BEGINNING$/i.test(s)) return 0;
    if (/^NOW$/i.test(s)) return feed.log.length;
    let m = /^CURSOR\s+'((?:''|[^'])*)'$/i.exec(s);
    if (m) return this.positionOf(feed, m[1].replace(/''/g, "'"));
    m = /^TIMESTAMP\s+(-?\d+)$/i.exec(s);
    if (m) {
      const nanos = BigInt(m[1]);
      return feed.log.filter((r) => r.eventTimeNs < nanos).length;
    }
    return refuse(400, `unreadable START AT: ${s}`);
  }

  private commands(body: Json): Json {
    const results = asList(body.commands).map((command) => this.command(asObj(command)));
    return this.execution(str(body.request_id) || undefined, results);
  }

  private command(command: Json): Json {
    const verb = str(command.command);
    const ok = (data: Json = {}): Json => ({ statement: verb, message: "ok", data });
    switch (verb) {
      case "define_state_store": {
        const store = this.store(str(command.name));
        for (const key of asList(command.named_keys)) {
          store.declaredKeys.add(str(asObj(key).name));
        }
        return ok({ store_id: store.id });
      }
      case "define_pipe": {
        const name = str(command.name);
        const subscription = this.subscriptions.get(str(command.subscription));
        const pipe: Json = {
          name,
          pipe_id: randomUUID(),
          stage: "declared",
          subscription_id: subscription?.id ?? randomUUID(),
          operation: asObj(command.operation),
          output_feed_id:
            command.output_feed != null ? this.feed(str(command.output_feed)).id : null,
          output_store_id:
            command.output_store != null ? this.store(str(command.output_store)).id : null,
          status: "running",
          created_at_ns: nowNs().toString(10),
        };
        this.pipes.set(name, pipe);
        return ok();
      }
      case "pause_pipe": {
        const pipe = this.pipes.get(str(command.name));
        if (pipe) pipe.stage = "paused";
        return ok();
      }
      case "resume_pipe": {
        const pipe = this.pipes.get(str(command.name));
        if (pipe) pipe.stage = "declared";
        return ok();
      }
      case "set_schema": {
        const scope = command.scope as Json;
        this.schemas.set(
          String(scope.kind) + ":" + String(scope.name),
          { ...(command.schema as Json) },
        );
        return ok();
      }
      case "clear_schema": {
        const scope = command.scope as Json;
        this.schemas.delete(String(scope.kind) + ":" + String(scope.name));
        return ok();
      }
      case "describe": {
        return ok(this.describe(str(command.kind).toUpperCase(), str(command.name)));
      }
      case "show": {
        return ok(this.show(str(command.kind).toUpperCase()) as unknown as Json);
      }
      case "drop": {
        const kind = str(command.kind);
        if (kind === "state_store") this.stores.delete(str(command.name));
        else if (kind === "pipe") this.pipes.delete(str(command.name));
        else this.drop(kind.toUpperCase(), str(command.name));
        return ok();
      }
      default:
        return ok();
    }
  }

  // ---- Writers -----------------------------------------------------------------

  private resolveWriter(name: string, epoch: number): FakeWriter {
    let writer = this.writers.get(name);
    if (!writer) {
      writer = new FakeWriter(name, this.implicitFeed().name);
      this.writers.set(name, writer);
    }
    if (writer.epoch === 0) writer.epoch = epoch; // implicit open
    if (epoch !== writer.epoch) {
      refuse(
        409,
        `writer '${name}' fenced: session epoch ${epoch} superseded by ${writer.epoch}`,
        "stale_epoch",
      );
    }
    return writer;
  }

  private appendReply(record: StoredRecord, deduplicated: boolean): Json {
    return {
      message_id: record.messageId,
      cursor: record.cursor,
      deduplicated,
      durability: "committed",
      feedback: { ...FEEDBACK },
    };
  }

  private writerAppend(body: Json): Json {
    const writer = this.resolveWriter(str(body.writer), int(body.session_epoch, 0));
    const dedupKey = `${writer.epoch}:${str(body.request_id)}`;
    const seen = writer.dedup.get(dedupKey);
    if (seen) return this.appendReply(seen, true);
    const record = this.feed(writer.feed).commit({
      messageId: randomUUID(),
      producerId: writer.id,
      sequence: ++writer.sequence,
      eventTimeNs: body.event_time_ns != null ? BigInt(str(body.event_time_ns)) : null,
      keyBase64: body.key_base64 == null ? null : str(body.key_base64),
      payloadBase64: str(body.payload_base64),
      metadataBase64: Object.fromEntries(
        Object.entries(asObj(body.metadata_base64)).map(([k, v]) => [k, str(v)]),
      ),
    });
    writer.dedup.set(dedupKey, record);
    return this.appendReply(record, false);
  }

  private writerAppendBatch(body: Json): Json {
    const results = asList(body.records).map((record) => this.writerAppend(asObj(record)));
    return { results, feedback: { ...FEEDBACK } };
  }

  private feedAppend(body: Json): Json {
    const feed = this.feed(str(body.feed));
    const sessionId = str(body.writer_session_id);
    const sequence = int(body.sequence, 0);
    const seqKey = `${feed.name}:${sessionId}`;
    const dedupKey = `${seqKey}:${str(body.request_id)}`;
    const seen = this.feedDedup.get(dedupKey);
    if (seen) return this.appendReply(seen, true);
    const last = this.feedSequences.get(seqKey);
    if (last !== undefined && sequence !== last + 1) {
      refuse(
        409,
        `writer sequence ${sequence} stale after ${last} on '${feed.name}'`,
        "sequence",
      );
    }
    const record = feed.commit({
      messageId: randomUUID(),
      producerId: sessionId || randomUUID(),
      sequence,
      eventTimeNs: body.event_time_ns != null ? BigInt(str(body.event_time_ns)) : null,
      keyBase64: body.key_base64 == null || str(body.key_base64) === "" ? null : str(body.key_base64),
      payloadBase64: str(body.payload_base64),
      metadataBase64: Object.fromEntries(
        Object.entries(asObj(body.metadata_base64)).map(([k, v]) => [k, str(v)]),
      ),
    });
    this.feedSequences.set(seqKey, sequence);
    this.feedDedup.set(dedupKey, record);
    return this.appendReply(record, false);
  }

  // ---- Readers -----------------------------------------------------------------

  private resolveReader(name: string): FakeReader {
    let reader = this.readers.get(name);
    if (!reader) {
      reader = new FakeReader(name, this.implicitFeed().name);
      this.readers.set(name, reader);
    }
    return reader;
  }

  private fence(reader: FakeReader, body: Json): void {
    const epoch = int(body.session_epoch, 0);
    if (!reader.open || epoch !== reader.sessionEpoch) {
      refuse(
        409,
        `reader '${reader.name}' fenced: session epoch ${epoch} ` +
          `superseded by ${reader.sessionEpoch}`,
        "stale_epoch",
      );
    }
  }

  /** Identical request_id + body → replay the recorded response. */
  private replay(reader: FakeReader, body: Json): Json | null {
    const seen = reader.dedup.get(str(body.request_id));
    return seen && seen.body === JSON.stringify(body) ? seen.response : null;
  }

  private remember(reader: FakeReader, body: Json, response: Json): Json {
    reader.dedup.set(str(body.request_id), { body: JSON.stringify(body), response });
    return response;
  }

  private sessionJson(reader: FakeReader): Json {
    const feed = this.feed(reader.feed);
    return {
      reader_id: reader.id,
      feed_id: feed.id,
      session_epoch: reader.sessionEpoch,
      capacity: reader.capacity,
      delivered_cursor: reader.delivered > 0 ? feed.cursorAt(reader.delivered) : null,
      acknowledged_cursor:
        reader.acknowledged > 0 ? feed.cursorAt(reader.acknowledged) : null,
    };
  }

  private readerOpen(body: Json): Json {
    const reader = this.resolveReader(str(body.reader));
    const seen = this.replay(reader, body);
    if (seen) return seen;
    reader.sessionEpoch += 1;
    reader.capacity = int(body.capacity, 64) || 64;
    reader.open = true;
    reader.delivered = reader.acknowledged; // resume at durable progress
    return this.remember(reader, body, this.sessionJson(reader));
  }

  private recordWire(feed: FakeFeed, record: StoredRecord): Json {
    return {
      cursor: record.cursor,
      message_id: record.messageId,
      producer_id: record.producerId,
      sequence: record.sequence,
      event_time_ns: record.eventTimeNs.toString(10),
      ingest_time_ns: record.ingestTimeNs.toString(10),
      key_base64: record.keyBase64,
      payload_base64: record.payloadBase64,
      metadata_base64: record.metadataBase64,
      source: {
        feed_name: feed.name.split(".").at(-1) ?? feed.name,
        full_feed_name: feed.name,
        feed_id: feed.id,
      },
    };
  }

  private readerFetch(body: Json): Json {
    const reader = this.resolveReader(str(body.reader));
    const seen = this.replay(reader, body);
    if (seen) return seen;
    this.fence(reader, body);
    const feed = this.feed(reader.feed);
    const limit = Math.min(int(body.limit, reader.capacity) || reader.capacity, 1000);
    const records = feed.log.slice(reader.delivered, reader.delivered + limit);
    reader.delivered += records.length;
    return this.remember(reader, body, {
      records: records.map((r) => this.recordWire(feed, r)),
      delivered_cursor: reader.delivered > 0 ? feed.cursorAt(reader.delivered) : null,
      acknowledged_cursor:
        reader.acknowledged > 0 ? feed.cursorAt(reader.acknowledged) : null,
      recommended_capacity: reader.capacity,
      retry_after_ms: 0,
    });
  }

  private readerAck(body: Json): Json {
    const reader = this.resolveReader(str(body.reader));
    const seen = this.replay(reader, body);
    if (seen) return seen;
    this.fence(reader, body);
    const feed = this.feed(reader.feed);
    reader.acknowledged = this.positionOf(feed, str(body.cursor));
    return this.remember(reader, body, this.sessionJson(reader));
  }

  private readerClose(body: Json): Json {
    const reader = this.resolveReader(str(body.reader));
    const seen = this.replay(reader, body);
    if (seen) return seen;
    this.fence(reader, body);
    reader.open = false; // delivered frontier dies with the session
    return this.remember(reader, body, this.sessionJson(reader));
  }

  private async temporaryFetch(body: Json): Promise<Json> {
    const feed = this.feed(str(body.feed));
    let position = 0;
    if (body.after != null) position = this.positionOf(feed, str(body.after));
    if (body.new_only === true) position = feed.log.length;
    if (body.after_event_time_ns != null) {
      const nanos = BigInt(str(body.after_event_time_ns));
      position = feed.log.filter((r) => r.eventTimeNs <= nanos).length;
    }
    const limit = Math.min(int(body.limit, 256) || 256, 1000);
    let records = feed.log.slice(position, position + limit);
    if (records.length === 0 && body.tail === true) {
      await feed.waitForAppend(Math.min(int(body.wait_ms, 500), 2000));
      records = feed.log.slice(position, position + limit);
    }
    const next = records.length > 0 ? records[records.length - 1].position : position;
    return {
      records: records.map((r) => this.recordWire(feed, r)),
      next_cursor: next > 0 ? feed.cursorAt(next) : null,
    };
  }

  private feedRecords(query: URLSearchParams): Json[] {
    const feed = this.feed(str(query.get("feed")));
    const after = query.get("after");
    const position = after !== null ? this.positionOf(feed, after) : 0;
    const limit = Math.min(int(query.get("limit"), 1000) || 1000, 1000);
    return feed.log
      .slice(position, position + limit)
      .map((r) => this.recordWire(feed, r));
  }

  // ---- StateStores --------------------------------------------------------------

  private mutation(store: FakeStore, body: Json, hash: string): Json | null {
    const mutationId = str(body.mutation_id);
    const seen = store.mutations.get(mutationId);
    if (seen) {
      if (seen.hash !== hash) {
        refuse(
          409,
          `mutation_id '${mutationId}' reused with different content`,
          "conflict",
        );
      }
      return { ...seen.result, deduplicated: true };
    }
    return null;
  }

  private commitMutation(store: FakeStore, body: Json, hash: string): Json {
    store.sequence += 1;
    const result: Json = {
      store_id: store.id,
      mutation_id: str(body.mutation_id),
      sequence: store.sequence,
      deduplicated: false,
    };
    store.mutations.set(str(body.mutation_id), { hash, result });
    return result;
  }

  private storeMutate(body: Json): Json {
    const store = this.store(str(body.store));
    const op = asObj(body.op);
    const hash = JSON.stringify(op);
    const seen = this.mutation(store, body, hash);
    if (seen) return seen;
    const kind = str(op.kind);
    if (kind !== "put_row" && kind !== "delete_row") {
      refuse(400, `unknown mutation op: ${kind}`);
    }
    store.sequence += 1;
    if (kind === "put_row") {
      const named: Record<string, unknown[]> = {};
      for (const entry of asList(op.named)) {
        const e = asObj(entry);
        const name = str(e.name);
        named[name] = asList(e.values);
        store.declaredKeys.add(name); // auto-declare keys a put carries
      }
      store.rows.set(str(op.key_base64), {
        keyBase64: str(op.key_base64),
        payloadBase64: str(op.payload_base64),
        named,
        appliedSeq: store.sequence,
      });
    } else {
      store.rows.delete(str(op.key_base64));
    }
    const result: Json = {
      store_id: store.id,
      mutation_id: str(body.mutation_id),
      sequence: store.sequence,
      deduplicated: false,
    };
    store.mutations.set(str(body.mutation_id), { hash, result });
    return result;
  }

  private storeLookup(body: Json): Json {
    const store = this.store(str(body.store));
    const row = store.rows.get(str(body.key_base64));
    return {
      store_id: store.id,
      found: row !== undefined,
      payload_base64: row?.payloadBase64 ?? null,
      applied_cursor: row ? storeCursor(row.appliedSeq) : null,
      applied_seq: store.sequence,
      committed_seq: store.sequence,
      behind: false,
      served_by: NODE_ID,
    };
  }

  private storeGetBy(body: Json): Json {
    const store = this.store(str(body.store));
    const namedKey = str(body.named_key);
    if (!store.declaredKeys.has(namedKey)) {
      refuse(400, `undeclared named key '${namedKey}' on store '${store.name}'`);
    }
    const wanted = asList(body.values).map(canonicalValue);
    const limit = Math.min(int(body.limit, 64) || 64, 1000);
    const rows = [...store.rows.values()]
      .filter((row) => sameValues(row.named[namedKey], wanted))
      .slice(0, limit)
      .map((row) => ({
        key_base64: row.keyBase64,
        payload_base64: row.payloadBase64,
        applied_cursor: storeCursor(row.appliedSeq),
      }));
    return {
      store_id: store.id,
      rows,
      applied_seq: store.sequence,
      committed_seq: store.sequence,
      behind: false,
      served_by: NODE_ID,
    };
  }

  private storeIterate(body: Json): Json {
    const store = this.store(str(body.store));
    const sorted = [...store.rows.values()].sort((a, b) =>
      Buffer.compare(Buffer.from(a.keyBase64, "base64"), Buffer.from(b.keyBase64, "base64")),
    );
    let start = 0;
    if (body.after_key_base64 != null) {
      const after = Buffer.from(str(body.after_key_base64), "base64");
      start = sorted.findIndex(
        (row) => Buffer.compare(Buffer.from(row.keyBase64, "base64"), after) > 0,
      );
      if (start < 0) start = sorted.length;
    }
    const limit = Math.min(int(body.limit, 256) || 256, 1000);
    const page = sorted.slice(start, start + limit);
    const more = start + page.length < sorted.length;
    return {
      store_id: store.id,
      rows: page.map((row) => ({
        key_base64: row.keyBase64,
        payload_base64: row.payloadBase64,
        applied_seq: row.appliedSeq,
        applied_cursor: storeCursor(row.appliedSeq),
      })),
      next_key_base64: more ? page[page.length - 1].keyBase64 : null,
      pinned_seq: store.sequence,
      served_by: NODE_ID,
    };
  }

  private storeClear(body: Json): Json {
    const store = this.store(str(body.store));
    const hash = `clear:${store.name}`;
    const seen = this.mutation(store, body, hash);
    if (seen) return seen;
    store.rows.clear();
    return this.commitMutation(store, body, hash);
  }
}

/** Canonical form of a wire typed value — the postings-match key. */
function canonicalValue(value: unknown): string {
  const v = asObj(value);
  return `${str(v.kind)}:${v.value !== undefined ? JSON.stringify(v.value) : str(v.value_base64)}`;
}

function sameValues(posting: unknown[] | undefined, wanted: string[]): boolean {
  if (posting === undefined || posting.length !== wanted.length) return false;
  return posting.every((v, i) => canonicalValue(v) === wanted[i]);
}

const storeCursor = (seq: number): string => String(seq).padStart(20, "0");
const nowNs = (): bigint => BigInt(Date.now()) * 1_000_000n;

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return refuse(400, "malformed JSON body");
  }
}

function reply(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status;
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(body ?? null));
}

/**
 * Start a loopback {@link FakeRiverbed} for tests:
 *
 * ```ts
 * const riverbed = await createFakeRiverbed();
 * const client = new WhitewaterClient({ apiKey: "k", endpoints: riverbed.endpoints });
 * ```
 */
export async function createFakeRiverbed(
  options: FakeRiverbedOptions = {},
): Promise<FakeRiverbed> {
  return FakeRiverbed.start(options);
}

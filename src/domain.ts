import { codecs, type Codec } from "./codecs.js";
import { Refused, WhitewaterError } from "./errors.js";
import { Feed, FeedReader, FeedWriter } from "./feed.js";
import type { ControlExecution, ReaderStart } from "./models.js";
import { ReaderStart as readerStart } from "./models.js";
import type { WhitewaterClient } from "./clients.js";

/** Lowercase dotted names: `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`, ≤512 bytes. */
function dotted(name: string): string {
  if (
    name.length > 512 ||
    name.startsWith(".") ||
    name.endsWith(".") ||
    !/^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$/.test(name)
  ) {
    throw new Error(`invalid dotted name: ${name}`);
  }
  return name;
}

type Row = Record<string, unknown>;

interface Snapshot {
  domainId: string;
  describe: Row;
  revision: number;
  feeds: string[];
  feedIds: Set<string>;
  readers: string[];
  writers: string[];
  subscriptions: string[];
  pipes: string[];
  indexes: string[];
  stateStores: string[];
}

/**
 * A strongly-typed handle on one Domain — a Domain owns every Feed it
 * prefixes (`shop` owns `shop.orders`), so the friendliest surface
 * navigates domain → feed → reader/writer instead of composing dotted
 * names by hand:
 *
 * ```ts
 * const shop = await client.getDomain("shop");       // verifies + loads now
 * const lazy = client.domain("shop");                // no I/O until needed
 *
 * shop.feedNames();                    // ['shop.orders', 'shop.returns']
 * shop.feeds("order");                 // prefix-filtered Feed handles
 * shop.readers(); shop.writers();      // scoped catalog views
 *
 * const reader = await client
 *   .createReader(shop, shop.feed("orders"))
 *   .readerId("fulfillment")                        // the Kafka groupId analogue
 *   .readerStartPoint(ReaderStart.BEGINNING)
 *   .capacity(256)
 *   .open();                                        // FeedReader<string, Buffer>
 * ```
 *
 * The first catalog accessor issues ONE batched control read
 * (`describe domain` + every `show` listing), joins rows by
 * `space_id`/`feed_id`, and caches the snapshot — {@link refresh}
 * re-pulls. `domain.feed("orders")` is pure name composition: zero I/O.
 */
export class Domain {
  private snapshot: Snapshot | null = null;

  /** @internal — bind via {@link WhitewaterClient.domain}. */
  constructor(
    private readonly client: WhitewaterClient,
    readonly name: string,
  ) {
    this.name = dotted(name);
  }

  /** The Domain's immutable id — survives renames. */
  get domainId(): Promise<string> {
    return this.snap().then((s) => s.domainId);
  }

  /** The catalog revision the snapshot was taken at. */
  get revision(): Promise<number> {
    return this.snap().then((s) => s.revision);
  }

  /** The raw `DESCRIBE DOMAIN` row — status, created_at_ns, history_policy. */
  async describe(): Promise<Row> {
    return (await this.snap()).describe;
  }

  /** The declared History Policy row, or null when unset. */
  async historyPolicy(): Promise<Row | null> {
    return ((await this.snap()).describe.history_policy as Row | undefined) ?? null;
  }

  // ---- scoped resources -------------------------------------------------

  /** Every Feed in this Domain as typed `Feed` handles, sorted by name.
   * `prefix` filters on the relative name (`shop.orders` → `orders`). */
  async feeds(prefix?: string): Promise<Feed<string, Buffer>[]> {
    return (await this.snap()).feeds
      .filter((full) => prefix === undefined || this.relative(full).startsWith(prefix))
      .map((full) => this.feed(this.relative(full)));
  }

  /** The Feed names in this Domain, sorted. */
  async feedNames(): Promise<string[]> {
    return [...(await this.snap()).feeds];
  }

  /** Declared Readers on this Domain's Feeds, sorted. */
  async readers(): Promise<string[]> {
    return [...(await this.snap()).readers];
  }

  /** Declared Writers on this Domain's Feeds, sorted. */
  async writers(): Promise<string[]> {
    return [...(await this.snap()).writers];
  }

  /** Declared Subscriptions in this Domain, sorted. */
  async subscriptions(): Promise<string[]> {
    return [...(await this.snap()).subscriptions];
  }

  /** Declared Pipes in this Domain, sorted. */
  async pipes(): Promise<string[]> {
    return [...(await this.snap()).pipes];
  }

  /** Declared Indexes in this Domain, sorted. */
  async indexes(): Promise<string[]> {
    return [...(await this.snap()).indexes];
  }

  /** Declared StateStores in this Domain, sorted. */
  async stateStores(): Promise<string[]> {
    return [...(await this.snap()).stateStores];
  }

  /** Re-pull the catalog snapshot — the next accessor sees fresh rows. */
  async refresh(): Promise<Domain> {
    this.snapshot = await this.load();
    return this;
  }

  // ---- Feed handles ------------------------------------------------------

  /**
   * A typed {@link Feed} handle on `relative` inside this Domain:
   * `shop.feed("orders")` binds `shop.orders`. An already-qualified name
   * (`"shop.orders"`) is accepted as-is. Pure name composition — no I/O.
   * Codecs bind exactly like {@link WhitewaterClient.feed}.
   */
  feed(relative: string): Feed<string, Buffer>;
  feed<V>(relative: string, valueCodec: Codec<V>): Feed<string, V>;
  feed<K, V>(relative: string, keyCodec: Codec<K>, valueCodec: Codec<V>): Feed<K, V>;
  feed<K, V>(
    relative: string,
    a?: Codec<K> | Codec<V>,
    b?: Codec<V>,
  ): Feed<K, V> | Feed<string, V> | Feed<string, Buffer> {
    const keyCodec = b !== undefined ? (a as Codec<K>) : (codecs.utf8() as unknown as Codec<K>);
    const valueCodec = (b ?? a ?? codecs.bytes()) as Codec<V>;
    return new Feed<K, V>(this.client, this.resolve(relative), keyCodec, valueCodec);
  }

  /** Alias of {@link feed}. */
  getFeed(relative: string): Feed<string, Buffer> {
    return this.feed(relative);
  }

  private resolve(relative: string): string {
    if (relative == null) throw new Error("feed name is required");
    const full = relative.startsWith(this.name + ".") ? relative : `${this.name}.${relative}`;
    return dotted(full);
  }

  private relative(full: string): string {
    return full.startsWith(this.name + ".") ? full.slice(this.name.length + 1) : full;
  }

  // ---- snapshot loading --------------------------------------------------

  private snap(): Promise<Snapshot> {
    if (this.snapshot !== null) {
      return Promise.resolve(this.snapshot);
    }
    return this.load().then((loaded) => {
      this.snapshot ??= loaded;
      return this.snapshot;
    });
  }

  /**
   * One batched control read: `describe domain` plus every `show`
   * listing. Rows join on `space_id` — readers/writers join through
   * their Feed's `feed_id`. A missing Domain fails the whole batch
   * server-side.
   */
  private async load(): Promise<Snapshot> {
    const execution = await this.client.commands([
      { command: "describe", kind: "domain", name: this.name },
      { command: "show", kind: "feeds" },
      { command: "show", kind: "readers" },
      { command: "show", kind: "writers" },
      { command: "show", kind: "subscriptions" },
      { command: "show", kind: "pipes" },
      { command: "show", kind: "indexes" },
      { command: "show", kind: "state_stores" },
    ]);
    const results = execution.results;
    const describe = (results[0]?.data ?? {}) as Row;
    const domainId = describe.space_id;
    if (typeof domainId !== "string" || domainId === "") {
      throw new Refused(404, "domain.unknown", `Domain '${this.name}' is not declared`);
    }

    const feeds: string[] = [];
    const feedIds = new Set<string>();
    for (const row of rows(results[1]?.data)) {
      if (row.space_id === domainId) {
        feeds.push(String(row.name));
        feedIds.add(String(row.feed_id));
      }
    }
    feeds.sort();

    const scoped = (index: number, field: string, ids: Set<string>): string[] =>
      rows(results[index]?.data)
        .filter((row) => ids.has(String(row[field])) && row.name != null)
        .map((row) => String(row.name))
        .sort();

    return {
      domainId,
      describe,
      revision: execution.revision,
      feeds,
      feedIds,
      readers: scoped(2, "feed_id", feedIds),
      writers: scoped(3, "feed_id", feedIds),
      subscriptions: scoped(4, "space_id", new Set([domainId])),
      pipes: scoped(5, "space_id", new Set([domainId])),
      indexes: scoped(6, "space_id", new Set([domainId])),
      stateStores: scoped(7, "space_id", new Set([domainId])),
    };
  }
}

function rows(data: unknown): Row[] {
  return Array.isArray(data) ? (data as Row[]) : [];
}

/** Fluent declaration + session-open of a durable named Reader —
 * returned by `client.createReader(domain, feed)`.
 *
 * `readerId` is required: it is the durable consume identity (the
 * Kafka `groupId` analogue). `readerStartPoint` applies only when the
 * Reader is first declared — an existing Reader resumes its
 * acknowledged frontier (the `auto.offset.reset` mapping). */
export class ReaderBuilder<K, V> {
  private id: string | null = null;
  private start: ReaderStart = readerStart.beginning();
  private pageCapacity = 128;

  /** @internal — build via {@link WhitewaterClient.createReader}. */
  constructor(
    private readonly client: WhitewaterClient,
    private readonly domain: Domain,
    private readonly bound: Feed<K, V>,
  ) {}

  /** The durable Reader identity. Required before open/declare. */
  readerId(readerId: string): this {
    this.id = readerId;
    return this;
  }

  /** First-time start position — `ReaderStart.BEGINNING`, `.END`,
   * `cursor(c)`, `timestamp(date)`, or `timestampNs(n)`. */
  readerStartPoint(start: ReaderStart): this {
    if (start == null) throw new Error("start is required");
    this.start = start;
    return this;
  }

  /** Bounded fetch page capacity — the default is 128. */
  capacity(capacity: number): this {
    if (!(capacity > 0)) throw new Error("capacity must be positive");
    this.pageCapacity = capacity;
    return this;
  }

  /** Declare the Reader in the catalog without opening a session —
   * fails when the name is already declared. */
  declare(): Promise<ControlExecution> {
    return this.client.createReader(this.requireId(), this.bound.name, this.start);
  }

  /** Declare the Reader when missing (an existing declaration keeps
   * its start and durable progress), open a session, and bind the
   * Feed's codecs — `fetch` returns decoded `FeedMessage`s. */
  open(): Promise<FeedReader<K, V>> {
    return this.bound.reader(this.requireId(), this.pageCapacity, this.start);
  }

  private requireId(): string {
    if (this.id === null) {
      throw new Error(
        "readerId(...) is required — the durable consume identity" +
          " (the Kafka groupId analogue)",
      );
    }
    return this.id;
  }
}

/** Fluent declaration + session-open of a durable named Writer —
 * returned by `client.createWriter(domain, feed)`. */
export class WriterBuilder<K, V> {
  private id: string | null = null;

  /** @internal — build via {@link WhitewaterClient.createWriter}. */
  constructor(
    private readonly client: WhitewaterClient,
    private readonly domain: Domain,
    private readonly bound: Feed<K, V>,
  ) {}

  /** The durable Writer identity. Required before open/declare. */
  writerId(writerId: string): this {
    this.id = writerId;
    return this;
  }

  /** Declare the Writer in the catalog without opening a session —
   * fails when the name is already declared. */
  declare(): Promise<ControlExecution> {
    return this.client.createWriter(this.requireId(), this.bound.name);
  }

  /** Declare the Writer when missing, open a session (the fencing
   * epoch stays inside the handle), and bind the Feed's codecs. */
  open(): Promise<FeedWriter<K, V>> {
    return this.bound.writer(this.requireId());
  }

  private requireId(): string {
    if (this.id === null) {
      throw new Error("writerId(...) is required");
    }
    return this.id;
  }
}

/** Coerce a `Feed` handle or relative name into a Feed inside
 * `domain` — a Feed bound to another Domain fails locally. */
export function asFeed<K, V>(domain: Domain, feed: Feed<K, V> | string): Feed<K, V> {
  const bound = feed instanceof Feed ? feed : (domain.feed(feed) as unknown as Feed<K, V>);
  if (!bound.name.startsWith(domain.name + ".")) {
    throw new Error(`feed '${bound.name}' is outside Domain '${domain.name}'`);
  }
  return bound;
}

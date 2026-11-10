import { codecs, type Codec } from "./codecs.js";
import { Refused, WhitewaterError } from "./errors.js";
import { Feed, FeedReader, FeedWriter } from "./feed.js";
import type { ControlExecution, FeedRecord, ReaderStart } from "./models.js";
import { ReaderStart as readerStart } from "./models.js";
import { Pipe } from "./operations.js";
import type { IndexPage, IndexPredicate, PipeInfo, PipeOperation } from "./operations.js";
import { StateStore } from "./clients.js";
import type { Subscription, SubscriptionMemberClient,
  ReaderSessionClient, WriterSessionClient, FeedClient } from "./clients.js";
import { Store } from "./store-view.js";
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

/** One side of a Feed's schema metadata — format plus optional
 * subject/definition text. Formats: `json`, `jsonschema`, `avro`,
 * `protobuf`, `text`, `bytes`, or `custom.<name>`. Records stay schema-neutral bytes on the
 * wire; declarations are catalog metadata for tooling and
 * compatibility — not yet an enforced registry. */
export interface SchemaDecl {
  format: string;
  subject?: string;
  definition?: string;
}

/** The declared key/value schema pair — either side may be absent. A
 * Feed's declaration overrides the named sides of its Domain default. */
export interface FeedSchema {
  key?: SchemaDecl;
  value?: SchemaDecl;
}

export function feedSchemaToWire(schema: FeedSchema): Row {
  const side = (decl: SchemaDecl | undefined): Row | undefined => {
    if (decl === undefined) return undefined;
    const out: Row = { format: decl.format };
    if (decl.subject !== undefined) out.subject = decl.subject;
    if (decl.definition !== undefined) out.definition = decl.definition;
    return out;
  };
  const out: Row = {};
  const key = side(schema.key);
  const value = side(schema.value);
  if (key !== undefined) out.key = key;
  if (value !== undefined) out.value = value;
  return out;
}

function schemaDeclFrom(row: unknown): SchemaDecl | undefined {
  if (row == null || typeof row !== "object") return undefined;
  const r = row as Row;
  return { format: String(r.format), ...(r.subject !== undefined ? { subject: String(r.subject) } : {}), ...(r.definition !== undefined ? { definition: String(r.definition) } : {}) };
}

function feedSchemaFrom(row: unknown): FeedSchema | null {
  if (row == null || typeof row !== "object") return null;
  const r = row as Row;
  const key = schemaDeclFrom(r.key);
  const value = schemaDeclFrom(r.value);
  if (key === undefined && value === undefined) return null;
  return { ...(key ? { key } : {}), ...(value ? { value } : {}) };
}

interface Snapshot {
  domainId: string;
  describe: Row;
  revision: number;
  feeds: string[];
  feedIds: Set<string>;
  feedSchemas: Map<string, FeedSchema | null>;
  declaredSchemas: Map<string, FeedSchema | null>;
  readers: string[];
  writers: string[];
  subscriptions: string[];
  pipes: string[];
  indexes: string[];
  stateStores: string[];
}

/**
 * An explicit endpoint reference — the cross-Domain vocabulary for
 * Pipe endpoints, a Subscription's source Feed, or a StateStore's
 * source. `scoped.rel("orders")` resolves inside the declaring Domain;
 * `scoped.abs("other.events")` keeps a qualified name. A `Feed`,
 * `Store`, `StateStore`, or `SubscriptionRef` handle also carries its
 * absolute name — the deliberate cross-Domain edge.
 */
export interface Scoped {
  readonly name: string;
  readonly absolute: boolean;
}

export const scoped = {
  /** A name inside the declaring Domain — the readable default. */
  rel(name: string): Scoped {
    return { name, absolute: false };
  },
  /** A fully-qualified `domain.name` — the deliberate cross-Domain edge. */
  abs(name: string): Scoped {
    return { name, absolute: true };
  },
};

/** Anything a Pipe endpoint, Subscription source, or StateStore source
 * accepts: a relative name (resolves in-domain), a {@link Scoped}, or a
 * handle carrying its absolute name. */
export type ScopedRef =
  | string
  | Scoped
  | Feed<unknown, unknown>
  | Store<unknown, unknown>
  | StateStore
  | SubscriptionRef;

/** A scoped reference to a declared Subscription — a Pipe input in
 * another declaration, or a member-lease target. */
export class SubscriptionRef {
  /** @internal */
  constructor(
    private readonly client: WhitewaterClient,
    /** The Subscription's qualified `domain.name`. */
    readonly name: string,
  ) {}

  /** The raw subscription-member client for this Subscription. */
  memberClient(): SubscriptionMemberClient {
    return this.client.subscriptionMember(this.name);
  }
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
 *   .maxPageSize(256)
 *   .open();                                        // FeedReader<string, string>
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

  /** The Domain's declared schema default — null when undeclared.
   * Every Feed without its own declaration inherits it. */
  async schema(): Promise<FeedSchema | null> {
    return feedSchemaFrom((await this.snap()).describe.schema);
  }

  /** The effective schema for a Feed — its declared sides layered over
   * the Domain default. Null when nothing is declared anywhere
   * (schema-neutral bytes). */
  async feedSchema(feed: string): Promise<FeedSchema | null> {
    return (await this.snap()).feedSchemas.get(feed) ?? null;
  }

  /** Only the sides the Feed itself declares — null when it inherits
   * the Domain default wholesale. */
  async declaredFeedSchema(feed: string): Promise<FeedSchema | null> {
    return (await this.snap()).declaredSchemas.get(feed) ?? null;
  }

  // ---- scoped resources -------------------------------------------------

  /** Every Feed in this Domain as typed `Feed` handles, sorted by name.
   * `prefix` filters on the relative name (`shop.orders` → `orders`). */
  async feeds(prefix?: string): Promise<Feed<string, string>[]> {
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
  feed(relative: string): Feed<string, string>;
  feed<V>(relative: string, valueCodec: Codec<V>): Feed<string, V>;
  feed<K, V>(relative: string, keyCodec: Codec<K>, valueCodec: Codec<V>): Feed<K, V>;
  feed<K, V>(
    relative: string,
    a?: Codec<K> | Codec<V>,
    b?: Codec<V>,
  ): Feed<K, V> | Feed<string, V> | Feed<string, string> {
    const keyCodec = b !== undefined ? (a as Codec<K>) : (codecs.utf8() as unknown as Codec<K>);
    const valueCodec = (b ?? a ?? codecs.utf8()) as Codec<V>;
    return new Feed<K, V>(this.client, this.resolve(relative), keyCodec, valueCodec);
  }

  /** Alias of {@link feed}. */
  getFeed(relative: string): Feed<string, string> {
    return this.feed(relative);
  }

  /**
   * `CREATE FEED` inside this Domain — `shop.createFeed("orders")`
   * declares `shop.orders`. {@link refresh} (or a lazy accessor's
   * first load) surfaces it through {@link feeds}/{@link feedNames}.
   */
  createFeed(relative: string): Promise<ControlExecution> {
    return this.client.createFeed(this.resolve(relative));
  }

  /**
   * Fluent Reader declaration on `feed` inside this Domain —
   * `shop.createReader("orders")` binds `shop.orders`:
   *
   * ```ts
   * const reader = await shop.createReader("orders")
   *   .readerId("fulfillment")               // the Kafka groupId analogue
   *   .readerStartPoint(ReaderStart.BEGINNING)
   *   .open();
   * ```
   *
   * Accepts a relative name or a `Feed` handle.
   */
  createReader<K, V>(feed: Feed<K, V> | string): ReaderBuilder<K, V> {
    return new ReaderBuilder<K, V>(this.client, this, asFeed(this, feed));
  }

  /**
   * Fluent Writer declaration on `feed` inside this Domain —
   * `shop.createWriter("orders")` binds `shop.orders`. Accepts a
   * relative name or a `Feed` handle.
   */
  createWriter<K, V>(feed: Feed<K, V> | string): WriterBuilder<K, V> {
    return new WriterBuilder<K, V>(this.client, this, asFeed(this, feed));
  }

  // ---- subscriptions, sessions, and the daily path -------------------------

  /**
   * `CREATE READER <name> FROM <feed>` inside this Domain —
   * `shop.createReaderFeed("audit", "orders")` declares `shop.audit`
   * consuming `shop.orders`. The feed argument is a {@link ScopedRef}:
   * a relative name resolves in-domain while a `Feed` handle or
   * `scoped.abs(...)` binds a Reader across Domains.
   */
  createReaderFeed(
    name: string,
    feed: ScopedRef,
    start: ReaderStart = readerStart.beginning(),
  ): Promise<ControlExecution> {
    return this.client.createReader(
      this.resolve(name), this.scopedName(feed), start);
  }

  /**
   * `CREATE WRITER <name> TO <feed>` inside this Domain — same
   * scoping as {@link createReaderFeed}.
   */
  createWriterFeed(
    name: string,
    feed: ScopedRef,
  ): Promise<ControlExecution> {
    return this.client.createWriter(
      this.resolve(name), this.scopedName(feed));
  }

  /**
   * `CREATE SUBSCRIPTION <name> FROM <feed>` inside this Domain — the
   * Subscription and its durable progress are owned here; the source
   * Feed may live in another Domain via its handle or `scoped.abs(...)`.
   */
  createSubscription(
    name: string,
    feed: ScopedRef,
    start: ReaderStart = readerStart.beginning(),
  ): Promise<ControlExecution> {
    return this.client.createSubscription(
      this.resolve(name), this.scopedName(feed), start);
  }

  /** A scoped reference to a Subscription inside this Domain — a Pipe
   * input in another declaration or a member-lease target. */
  subscription(relative: string): SubscriptionRef {
    return new SubscriptionRef(this.client, this.resolve(relative));
  }

  /** The raw subscription-member client inside this Domain —
   * join/claim/renew/release/fetch/ack under fenced leases. */
  memberClient(subscription: string): SubscriptionMemberClient {
    return this.client.subscriptionMember(this.resolve(subscription));
  }

  /** A named-Writer session inside this Domain — epoch from
   * {@link openWriterSession}. Prefer `Feed.writer` or the
   * `WriterBuilder` for the typed route. */
  writer(name: string, sessionEpoch: number): WriterSessionClient {
    return this.client.writer(this.resolve(name), sessionEpoch);
  }

  /** A named-Reader session inside this Domain —
   * open/fetch/ack/close with explicit session state. */
  reader(name: string): ReaderSessionClient {
    return this.client.reader(this.resolve(name));
  }

  /**
   * Subscribe durably inside this Domain: ensures the Reader exists
   * (creating it at `start` when missing), opens a session, and invokes
   * `onRecords` per fetched page in the background, acknowledging after
   * each batch. The feed may point at another Domain through a `Feed`
   * handle or `scoped.abs(...)`.
   */
  subscribe(
    reader: string,
    feed: ScopedRef,
    start: ReaderStart,
    onRecords: (records: FeedRecord[]) => void | Promise<void>,
    onError?: (error: WhitewaterError) => void,
  ): Subscription {
    return this.client.subscribe(
      this.resolve(reader), this.scopedName(feed), start,
      onRecords, onError);
  }

  /** `OPEN WRITER SESSION <name>` — the epoch comes back in the
   * execution's data. */
  openWriterSession(writer: string): Promise<ControlExecution> {
    return this.client.openWriterSession(this.resolve(writer));
  }

  /** `SEEK READER <name> TO <start>` inside this Domain. */
  seekReader(reader: string, start: ReaderStart): Promise<ControlExecution> {
    return this.client.seekReader(this.resolve(reader), start);
  }

  /** The byte-level `FeedClient` — committed reads, anonymous
   * temporary fetches, and ad-hoc appends. */
  feedOps(): FeedClient {
    return this.client.feeds;
  }

  // ---- resource lifecycle --------------------------------------------------

  /** `DESCRIBE FEED <name>` inside this Domain. */
  describeFeed(relative: string): Promise<ControlExecution> {
    return this.client.describeFeed(this.resolve(relative));
  }

  /** `DESCRIBE WRITER <name>` inside this Domain. */
  describeWriter(relative: string): Promise<ControlExecution> {
    return this.client.describeWriter(this.resolve(relative));
  }

  /** `DESCRIBE READER <name>` inside this Domain. */
  describeReader(relative: string): Promise<ControlExecution> {
    return this.client.describeReader(this.resolve(relative));
  }

  /** `DESCRIBE SUBSCRIPTION <name>` inside this Domain. */
  describeSubscription(relative: string): Promise<ControlExecution> {
    return this.client.describeSubscription(this.resolve(relative));
  }

  /** `RENAME FEED <current> TO <renamed>` — both names resolve in-domain. */
  renameFeed(current: string, renamed: string): Promise<ControlExecution> {
    return this.client.renameFeed(this.resolve(current), this.resolve(renamed));
  }

  /** `RENAME WRITER <current> TO <renamed>` in-domain. */
  renameWriter(current: string, renamed: string): Promise<ControlExecution> {
    return this.client.renameWriter(this.resolve(current), this.resolve(renamed));
  }

  /** `RENAME READER <current> TO <renamed>` in-domain. */
  renameReader(current: string, renamed: string): Promise<ControlExecution> {
    return this.client.renameReader(this.resolve(current), this.resolve(renamed));
  }

  /** `RENAME SUBSCRIPTION <current> TO <renamed>` in-domain. */
  renameSubscription(current: string, renamed: string): Promise<ControlExecution> {
    return this.client.renameSubscription(this.resolve(current), this.resolve(renamed));
  }

  /** `DROP FEED <name>` inside this Domain. */
  dropFeed(relative: string): Promise<ControlExecution> {
    return this.client.dropFeed(this.resolve(relative));
  }

  /** `DROP WRITER <name>` inside this Domain. */
  dropWriter(relative: string): Promise<ControlExecution> {
    return this.client.dropWriter(this.resolve(relative));
  }

  /** `DROP READER <name>` inside this Domain. */
  dropReader(relative: string): Promise<ControlExecution> {
    return this.client.dropReader(this.resolve(relative));
  }

  /** `DROP SUBSCRIPTION <name>` inside this Domain. */
  dropSubscription(relative: string): Promise<ControlExecution> {
    return this.client.dropSubscription(this.resolve(relative));
  }

  /** `SET SCHEMA ON FEED <feed>` inside this Domain. */
  setFeedSchema(relative: string, schema: FeedSchema): Promise<ControlExecution> {
    return this.client.setFeedSchema(this.resolve(relative), schema);
  }

  /** `DROP SCHEMA ON FEED <feed>` inside this Domain. */
  clearFeedSchema(relative: string): Promise<ControlExecution> {
    return this.client.clearFeedSchema(this.resolve(relative));
  }

  // ---- pipes, stores, indexes ----------------------------------------------

  /** The lifecycle handle for a Pipe inside this Domain —
   * describe/pause/resume/drop. */
  pipe(relative: string): Pipe {
    return this.client.pipe(this.resolve(relative));
  }

  /** Declared Pipes inside this Domain as `PipeInfo` rows. */
  async pipeInfos(): Promise<PipeInfo[]> {
    const all = await this.client.pipes();
    const prefix = this.name + ".";
    return all.filter((info) => info.name.startsWith(prefix));
  }

  /**
   * `DEFINE PIPE <name> ... TO FEED` inside this Domain — the copy
   * Pipe. Every endpoint is a {@link ScopedRef}: a bare string or
   * `scoped.rel(...)` resolves in-domain; a handle or `scoped.abs(...)`
   * crosses Domains. The input Subscription must belong to this Domain
   * (it is the Pipe's own durable frontier); the output Feed may live
   * anywhere.
   */
  defineForwardPipe(
    name: string,
    subscription: ScopedRef,
    outputFeed: ScopedRef,
  ): Promise<ControlExecution> {
    return this.client.defineForwardPipe(
      this.resolve(name), this.scopedName(subscription), this.scopedName(outputFeed));
  }

  /** `DEFINE PIPE ... ENRICH` inside this Domain — every endpoint
   * follows the {@link ScopedRef} rule. */
  defineEnrichPipe(
    name: string,
    subscription: ScopedRef,
    outputFeed: ScopedRef,
    store: ScopedRef,
    operation: PipeOperation,
    quarantineFeed?: ScopedRef,
  ): Promise<ControlExecution> {
    return this.client.defineEnrichPipe(
      this.resolve(name), this.scopedName(subscription),
      this.scopedName(outputFeed), this.scopedName(store),
      operation,
      quarantineFeed === undefined ? undefined : this.scopedName(quarantineFeed));
  }

  /** `DEFINE PIPE ... TO STATESTORE` inside this Domain — every
   * endpoint follows the {@link ScopedRef} rule. */
  defineWindowPipe(
    name: string,
    subscription: ScopedRef,
    outputStore: ScopedRef,
    operation: PipeOperation,
    emitFeed?: ScopedRef,
  ): Promise<ControlExecution> {
    return this.client.defineWindowPipe(
      this.resolve(name), this.scopedName(subscription),
      this.scopedName(outputStore), operation,
      emitFeed === undefined ? undefined : this.scopedName(emitFeed));
  }

  /** The byte-level StateStore facade inside this Domain —
   * `shop.stateStore("profiles")` binds `shop.profiles`. */
  stateStore(relative: string): StateStore {
    return this.client.stateStore(this.resolve(relative));
  }

  /**
   * A codec-bound Store facade inside this Domain:
   * `domain.store(name)` binds `Store<string, string>`; a second
   * argument types the payload; three arguments bind both codecs.
   */
  store(relative: string): Store<string, string>;
  store<V>(relative: string, valueCodec: Codec<V>): Store<string, V>;
  store<K, V>(relative: string, keyCodec: Codec<K>, valueCodec: Codec<V>): Store<K, V>;
  store<K, V>(
    relative: string,
    a?: Codec<K> | Codec<V>,
    b?: Codec<V>,
  ): Store<K, V> | Store<string, V> | Store<string, string> {
    const keyCodec = b !== undefined ? (a as Codec<K>) : (codecs.utf8() as unknown as Codec<K>);
    const valueCodec = (b ?? a ?? codecs.utf8()) as Codec<V>;
    return Store.bind(this.stateStore(relative), keyCodec, valueCodec);
  }

  /**
   * `DEFINE STATESTORE` inside this Domain — the spec's name resolves
   * in-domain (`StateStore.named("employees")` on `accounts` declares
   * `accounts.employees`); `source` names the Feed it materializes
   * from (undefined = manual) and may cross Domains via a `Feed`
   * handle or `scoped.abs(...)`.
   */
  async defineStateStore(spec: StateStore, source?: ScopedRef): Promise<StateStore> {
    const full = this.resolve(spec.name);
    const rescoped = StateStore.named(full);
    for (const key of spec.namedKeys) {
      rescoped.addKey(key.name, key.unique, key.extractor);
    }
    return this.client.stateStores.define(
      rescoped, source === undefined ? undefined : this.scopedName(source));
  }

  /** `DROP STATESTORE <name>` inside this Domain. */
  dropStateStore(relative: string): Promise<ControlExecution> {
    return this.client.stateStores.drop(this.resolve(relative));
  }

  /** Query a replicated Index inside this Domain — the index name
   * resolves in-domain (indexes stay same-Domain with their Feed). */
  queryIndex(
    relative: string,
    predicate: IndexPredicate,
    options: { limit?: number; strict?: boolean; waitMs?: number } = {},
  ): Promise<IndexPage> {
    return this.client.queryIndex(this.resolve(relative), predicate, options);
  }

  /**
   * Resolve a {@link ScopedRef} to its qualified wire name — bare
   * strings and `scoped.rel(...)` compose inside this Domain; handles
   * and `scoped.abs(...)` carry qualified names.
   */
  scopedName(ref: ScopedRef): string {
    if (typeof ref === "string") return this.resolve(ref);
    if (ref instanceof SubscriptionRef) return ref.name;
    if (typeof (ref as Scoped).absolute === "boolean") {
      const s = ref as Scoped;
      return s.absolute ? dotted(s.name) : this.resolve(s.name);
    }
    return (ref as { name: string }).name;
  }

  // ---- name resolution -------------------------------------------------------

  /** @internal — relative-name resolution; callers pass
   * {@link ScopedRef}s, never raw dotted strings. */
  resolve(relative: string): string {
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
    const feedSchemas = new Map<string, FeedSchema | null>();
    const declaredSchemas = new Map<string, FeedSchema | null>();
    for (const row of rows(results[1]?.data)) {
      if (row.space_id === domainId) {
        const name = String(row.name);
        feeds.push(name);
        feedIds.add(String(row.feed_id));
        feedSchemas.set(name, feedSchemaFrom(row.effective_schema));
        declaredSchemas.set(name, feedSchemaFrom(row.schema));
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
      feedSchemas,
      declaredSchemas,
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
  private pageSize = 128;

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

  /** Max records delivered per fetch page — the default is 128. */
  maxPageSize(maxPageSize: number): this {
    if (!(maxPageSize > 0)) throw new Error("maxPageSize must be positive");
    this.pageSize = maxPageSize;
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
    return this.bound.reader(this.requireId(), this.pageSize, this.start);
  }

  private requireId(): string {
    if (this.id === null) {
      throw new Error(
        "readerId(...) is required — the durable consume identity" +
          " (the Kafka groupId analogue)",
      );
    }
    // Reader identities are Domain resources — "fulfillment" inside
    // "shop" declares shop.fulfillment, never a global name.
    return this.domain.resolve(this.id);
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
    // Writer identities are Domain resources — same rule as readerId.
    return this.domain.resolve(this.id);
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

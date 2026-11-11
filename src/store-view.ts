import { randomUUID } from "node:crypto";
import { Codec } from "./codecs.js";
import type {
  ControlExecution,
  MutationResult,
  StoreFreshness,
  StorePage,
  StoreRow,
  StoreRows,
  StoreValue,
} from "./models.js";
import type { StateStore } from "./clients.js";

export interface StoreKeySet {
  toRecord(): Record<string, StoreValue | StoreValue[]>;
  toMap?(): Record<string, StoreValue | StoreValue[]>;
}

export abstract class AbstractStoreKeySet implements StoreKeySet {
  private readonly keys: Record<string, StoreValue | StoreValue[]> = {};

  put(name: string, value: StoreValue | StoreValue[]): this {
    if (!name || !name.trim()) {
      throw new Error("secondary key name must not be blank");
    }
    this.keys[name] = value;
    return this;
  }

  get(name: string): StoreValue | StoreValue[] | undefined {
    return this.keys[name];
  }

  get isEmpty(): boolean {
    return Object.keys(this.keys).length === 0;
  }

  toRecord(): Record<string, StoreValue | StoreValue[]> {
    return { ...this.keys };
  }

  toMap(): Record<string, StoreValue | StoreValue[]> {
    return this.toRecord();
  }
}

export class GenericStoreKeySet extends AbstractStoreKeySet {}

export type NamedKeysInput<KS extends StoreKeySet = GenericStoreKeySet> =
  | Record<string, StoreValue | StoreValue[]>
  | StoreKeySet
  | ((k: KS) => void)
  | string;

export function isStoreKeySet(value: unknown): value is StoreKeySet {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as StoreKeySet).toRecord === "function"
  );
}

export const StoreKeySet = {
  of(
    keyOrBuilder:
      | string
      | Record<string, StoreValue | StoreValue[]>
      | ((k: GenericStoreKeySet) => void),
    value?: StoreValue | StoreValue[],
    k2?: string,
    v2?: StoreValue | StoreValue[],
    k3?: string,
    v3?: StoreValue | StoreValue[],
  ): StoreKeySet {
    const ks = new GenericStoreKeySet();
    if (typeof keyOrBuilder === "function") {
      keyOrBuilder(ks);
      return ks;
    }
    if (typeof keyOrBuilder === "string") {
      ks.put(keyOrBuilder, value ?? null);
      if (k2 !== undefined) ks.put(k2, v2 ?? null);
      if (k3 !== undefined) ks.put(k3, v3 ?? null);
      return ks;
    }
    for (const [k, v] of Object.entries(keyOrBuilder)) {
      ks.put(k, v);
    }
    return ks;
  },
};


/** One StateStore row with key and payload decoded through the bound
 * codecs — the {@link StoreRow} fields carry through; only the values
 * are typed. */
export interface Row<K, V> {
  key: K;
  value: V;
  appliedCursor?: string;
  freshness?: StoreFreshness;
}

/** A named-key lookup result: typed rows plus freshness evidence. */
export interface RowsResult<K, V> {
  rows: Row<K, V>[];
  freshness: StoreFreshness;
}

/** One bounded typed iteration page in primary-key order; `nextKey`
 * continues the scan, `pinnedSeq` is the replica's applied journal
 * position. */
export interface RowPage<K, V> {
  rows: Row<K, V>[];
  nextKey: K | null;
  pinnedSeq: number;
  servedBy: string;
}

/**
 * A codec-bound facade over one node-side StateStore — the same shape
 * Java calls `StateStore<K,V>`. Every {@link get}/{@link put}/
 * {@link iterate} serializes through the bound codecs, so application
 * code stays in its own types:
 *
 * ```ts
 * const profiles = client.store("accounts.employees", codecs.json<Profile>());
 * await profiles.put("user-1", profile, { passport: "P111" });
 * const row = await profiles.get("user-1");      // row.value is Profile
 * for await (const r of profiles.iterate()) { }
 * ```
 *
 * Declare named keys on the raw spec first — Store binds codecs for
 * data operations, not declaration:
 *
 * ```ts
 * const spec = StateStore.named("accounts.employees").addKey("passport", true);
 * const bound = await client.stateStores.define(spec);
 * const profiles = Store.bind(bound, codecs.utf8(), codecs.json<Profile>());
 * ```
 */
export class Store<K, V, KS extends StoreKeySet = GenericStoreKeySet> {
  /** @internal — bind via {@link WhitewaterClient.store} or
   * {@link Store.bind}. */
  constructor(
    private readonly store: StateStore,
    private readonly keyCodec: Codec<K>,
    private readonly valueCodec: Codec<V>,
    private readonly keySetFactory: () => KS = () => new GenericStoreKeySet() as unknown as KS,
  ) {}

  /** Rebind an already-bound byte-level StateStore under typed codecs. */
  static bind<K, V>(
    store: StateStore,
    keyCodec: Codec<K>,
    valueCodec: Codec<V>,
  ): Store<K, V> {
    return new Store<K, V>(store, keyCodec, valueCodec);
  }

  /** The store's name. */
  get name(): string {
    return this.store.name;
  }

  /** The underlying byte-level StateStore. */
  raw(): StateStore {
    return this.store;
  }

  /** Rebind the same store under different codecs — no wire call:
   * `store.view(codecs.utf8(), codecs.bytes())` reads raw. */
  withDefaultKey(defaultKeyName: string): this {
    this.store.withDefaultKey(defaultKeyName);
    return this;
  }

  withKeySet<KS2 extends StoreKeySet>(
    ctorOrFactory: (new () => KS2) | (() => KS2),
  ): Store<K, V, KS2> {
    const factory =
      typeof ctorOrFactory === "function" && ctorOrFactory.prototype
        ? () => new (ctorOrFactory as new () => KS2)()
        : (ctorOrFactory as () => KS2);
    return new Store<K, V, KS2>(this.store, this.keyCodec, this.valueCodec, factory);
  }

  view<K2, V2>(keyCodec: Codec<K2>, valueCodec: Codec<V2>): Store<K2, V2, KS> {
    return new Store<K2, V2, KS>(this.store, keyCodec, valueCodec, this.keySetFactory);
  }

  put(
    key: K,
    value: V,
    named?: NamedKeysInput<KS>,
    mutationId = randomUUID(),
  ): Promise<MutationResult> {
    let resolved: Record<string, StoreValue | StoreValue[]> | StoreKeySet | string | undefined;
    if (typeof named === "function") {
      const ks = this.keySetFactory();
      named(ks);
      resolved = ks.toRecord();
    } else {
      resolved = named;
    }
    return this.store.put(
      this.keyCodec.encode(key),
      this.valueCodec.encode(value),
      resolved,
      mutationId,
    );
  }

  /** Primary-key read, decoded through the bound codecs; null when
   * absent. `strict` bounded-waits for the replica's committed
   * frontier. */
  async get(
    key: K,
    options: { strict?: boolean; waitMs?: number } = {},
  ): Promise<Row<K, V> | null> {
    const row = await this.store.get(this.keyCodec.encode(key), options);
    return row === null ? null : this.decodeRow(row);
  }

  /** Named-key lookup: every typed primary row the value points at plus
   * the serving replica's freshness evidence. */
  async getBy(
    namedKey: string,
    value: StoreValue | StoreValue[],
    options: { limit?: number; strict?: boolean; waitMs?: number } = {},
  ): Promise<RowsResult<K, V>> {
    const rows: StoreRows = await this.store.getBy(namedKey, value, options);
    return {
      rows: rows.rows.map((row) => this.decodeRow(row)),
      freshness: rows.freshness,
    };
  }

  /** Value-only primary lookup: the decoded value or null when the key
   * is absent. `get` remains for the row's freshness/applied-cursor
   * evidence. */
  async find(
    key: K,
    options: { strict?: boolean; waitMs?: number } = {},
  ): Promise<V | null> {
    const row = await this.get(key, options);
    return row === null ? null : row.value;
  }

  /** Value-only named-key lookup: the first matching row's decoded
   * value or null on no match. `getBy` remains for every match plus
   * freshness evidence. */
  async findBy(
    namedKey: string,
    value: StoreValue,
    options: { strict?: boolean; waitMs?: number } = {},
  ): Promise<V | null> {
    const rows = await this.getBy(namedKey, value, { ...options, limit: 1 });
    return rows.rows.length === 0 ? null : rows.rows[0].value;
  }

  /** Primary lookup returning `fallback` when the key is absent —
   * counters and flags become one line. */
  async getOrDefault(key: K, fallback: V): Promise<V> {
    const value = await this.find(key);
    return value === null ? fallback : value;
  }

  /** Remove the row and its named postings. */
  delete(key: K, mutationId = randomUUID()): Promise<MutationResult> {
    return this.store.delete(this.keyCodec.encode(key), mutationId);
  }

  /** One bounded typed iteration page in primary-key order; pass the
   * previous page's `nextKey` (or omit) to continue. */
  async page(afterKey?: K | null, limit?: number): Promise<RowPage<K, V>> {
    const page: StorePage = await this.store.page(
      afterKey == null ? undefined : this.keyCodec.encode(afterKey),
      limit,
    );
    return {
      rows: page.rows.map((row) => this.decodeRow(row)),
      nextKey: page.nextKey === null ? null : this.keyCodec.decode(page.nextKey),
      pinnedSeq: page.pinnedSeq,
      servedBy: page.servedBy,
    };
  }

  /** Async-iterate every row in primary-key order across bounded
   * pages. */
  async *iterate(limit?: number): AsyncGenerator<Row<K, V>> {
    let after: K | null = null;
    for (;;) {
      const page = await this.page(after, limit);
      for (const row of page.rows) yield row;
      if (page.nextKey === null) return;
      after = page.nextKey;
    }
  }

  /** Empty the store as one journaled mutation. */
  clear(mutationId = randomUUID()): Promise<MutationResult> {
    return this.store.clear(mutationId);
  }

  /** Tombstone the store. */
  drop(requestId?: string): Promise<ControlExecution> {
    return this.store.drop(requestId);
  }

  private decodeRow(row: StoreRow): Row<K, V> {
    const decoded: Row<K, V> = {
      key: this.keyCodec.decode(row.key),
      value: this.valueCodec.decode(row.payload),
      freshness: row.freshness,
    };
    if (row.appliedCursor !== undefined) decoded.appliedCursor = row.appliedCursor;
    return decoded;
  }
}

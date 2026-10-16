/**
 * Codec<T> is a bidirectional object ↔ wire-bytes serialization — the
 * wire always carries Buffers; a Codec keeps that plumbing inside the
 * SDK so application code works with its own types:
 *
 * ```ts
 * const orders = client.feed("orders.events", codecs.json<Order>());
 * const writer = await orders.writer("ingest");
 * await writer.append("order-1", { id: "order-1", total: 42 });
 * ```
 *
 * {@link codecs.utf8} for text keys/payloads, {@link codecs.bytes} for
 * raw binary, {@link codecs.json} for any JSON-mapped type, and
 * {@link codecs.custom} for anything else (protobuf, msgpack,
 * application framing).
 */
export interface Codec<T> {
  /** Serialize one value to wire bytes. */
  encode(value: T): Buffer;
  /** Deserialize wire bytes to a value. */
  decode(data: Buffer): T;
}

class Utf8Codec implements Codec<string> {
  encode(value: string): Buffer {
    return Buffer.from(value, "utf8");
  }
  decode(data: Buffer): string {
    return data.toString("utf8");
  }
}

class BytesCodec implements Codec<Buffer> {
  encode(value: Buffer): Buffer {
    return value;
  }
  decode(data: Buffer): Buffer {
    return data;
  }
}

class JsonCodec<T> implements Codec<T> {
  encode(value: T): Buffer {
    return Buffer.from(JSON.stringify(value), "utf8");
  }
  decode(data: Buffer): T {
    return JSON.parse(data.toString("utf8")) as T;
  }
}

class CustomCodec<T> implements Codec<T> {
  constructor(
    private readonly encoder: (value: T) => Buffer,
    private readonly decoder: (data: Buffer) => T,
  ) {}
  encode(value: T): Buffer {
    return this.encoder(value);
  }
  decode(data: Buffer): T {
    return this.decoder(data);
  }
}

export const codecs = {
  /** UTF-8 text — the common case for keys and JSON/XML/text payloads. */
  utf8: (): Codec<string> => new Utf8Codec(),

  /** Identity codec — raw Buffers through to the wire. */
  bytes: (): Codec<Buffer> => new BytesCodec(),

  /** JSON-mapped type — objects round-trip through JSON.stringify/parse. */
  json: <T>(): Codec<T> => new JsonCodec<T>(),

  /**
   * A caller-supplied pair — protobuf, msgpack, application framing:
   *
   * ```ts
   * const protobuf = codecs.custom<User>(
   *   (u) => Buffer.from(User.encode(u).finish()),
   *   (b) => User.decode(new Uint8Array(b)),
   * );
   * ```
   */
  custom: <T>(
    encode: (value: T) => Buffer,
    decode: (data: Buffer) => T,
  ): Codec<T> => new CustomCodec<T>(encode, decode),

  /**
   * Bigint values as fixed-width little-endian 8-byte fields — for
   * typed numeric keys (nanoseconds, ids) without JSON overhead.
   */
  int64: (): Codec<bigint> =>
    new CustomCodec<bigint>(
      (value) => {
        const buffer = Buffer.alloc(8);
        buffer.writeBigInt64LE(value);
        return buffer;
      },
      (data) => data.readBigInt64LE(0),
    ),
} as const;

/** Encode one text Metadata map to wire Buffers. */
export function textMetadata(
  metadata: Record<string, string>,
): Record<string, Buffer> {
  const encoded: Record<string, Buffer> = {};
  for (const [name, value] of Object.entries(metadata)) {
    encoded[name] = Buffer.from(value, "utf8");
  }
  return encoded;
}

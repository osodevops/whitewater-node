import { randomUUID } from "node:crypto";
import { Refused, SchemaValidation, Unavailable, WhitewaterError } from "./errors.js";
import type { Transport } from "./transport.js";

export const SchemaTarget = {
  KEY: "key",
  MESSAGE: "message",
  Key: "key",
  Message: "message",
} as const;
export type SchemaTarget = "key" | "message";

export const SchemaType = {
  JSON: "JSON",
  AVRO: "AVRO",
  PROTOBUF: "PROTOBUF",
  Json: "JSON",
  Avro: "AVRO",
  Protobuf: "PROTOBUF",
} as const;
export type SchemaType = "JSON" | "AVRO" | "PROTOBUF";

export const SchemaCompatibility = {
  NONE: "NONE",
  BACKWARD: "BACKWARD",
  BACKWARD_TRANSITIVE: "BACKWARD_TRANSITIVE",
  FORWARD: "FORWARD",
  FORWARD_TRANSITIVE: "FORWARD_TRANSITIVE",
  FULL: "FULL",
  FULL_TRANSITIVE: "FULL_TRANSITIVE",
} as const;
export type SchemaCompatibility =
  | "NONE"
  | "BACKWARD"
  | "BACKWARD_TRANSITIVE"
  | "FORWARD"
  | "FORWARD_TRANSITIVE"
  | "FULL"
  | "FULL_TRANSITIVE";

export interface SchemaVersion {
  version: number;
  type: SchemaType;
  definition: string;
  createdAtNs: bigint;
}

export interface SchemaEntry {
  key: string;
  resource: string;
  target: SchemaTarget;
  type: SchemaType;
  compatibility: SchemaCompatibility;
  version: number;
  definition: string;
  updatedAtNs: bigint;
  versions: SchemaVersion[];
  readOnly?: boolean;
  sourceKey?: string | null;
  sourceCluster?: string | null;
  linkedTargets?: string[];
}

export interface SchemaCompatibilityResult {
  compatible: boolean;
  messages: string[];
}

export const SCHEMA_STORE_DOMAIN = "whitewater";
export const SCHEMA_STORE_NAME = "whitewater.schema.registry";

export function isReservedSchemaStoreName(name: string | null | undefined): boolean {
  if (!name) return false;
  const norm = name.trim().toLowerCase();
  return (
    norm === SCHEMA_STORE_NAME ||
    norm === "schema.registry" ||
    norm === "schema-registry" ||
    norm === "whitewater.schema-registry"
  );
}

export function normalizeSchemaTarget(raw: string): SchemaTarget {
  const s = (raw ?? "").trim().toLowerCase();
  if (s === "key" || s === "k") return "key";
  if (s === "message" || s === "value" || s === "val" || s === "msg" || s === "m" || s === "v") {
    return "message";
  }
  throw new Error(`unknown schema target: ${raw}`);
}

export function normalizeSchemaType(raw: string): SchemaType {
  const s = (raw ?? "").trim().toUpperCase();
  if (s === "JSON" || s === "JSONSCHEMA" || s === "JSON_SCHEMA") return "JSON";
  if (s === "AVRO") return "AVRO";
  if (s === "PROTOBUF" || s === "PROTO" || s === "PROTO3" || s === "PROTO2") return "PROTOBUF";
  throw new Error(`unknown schema type: ${raw}`);
}

export function composeRegistryKey(resource: string, target: SchemaTarget): string {
  const r = resource.trim().split("|")[0].trim();
  if (!/^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$/.test(r)) {
    throw new Error(`invalid dotted name: ${resource}`);
  }
  return `${r}|${normalizeSchemaTarget(target)}`;
}

type CompiledValidator = (payload: Buffer) => string[];

const JSON_TYPES = new Set(["object", "array", "string", "number", "integer", "boolean", "null"]);
const AVRO_PRIMS = new Set(["null", "boolean", "int", "long", "float", "double", "bytes", "string"]);

export function validateSchemaDefinition(type: SchemaType, definition: string): string[] {
  const raw = (definition ?? "").trim();
  if (!raw) return [`${type} schema definition must not be empty`];
  if (type === "PROTOBUF") {
    const issues: string[] = [];
    if (!/syntax\s*=\s*['"]proto[23]['"]\s*;/.test(raw)) {
      issues.push('missing syntax declaration, e.g. syntax = "proto3";');
    }
    if (!/\b(message|enum)\s+[A-Za-z_]\w*/.test(raw)) {
      issues.push("schema defines no 'message' or 'enum'");
    }
    let depth = 0;
    for (const ch of raw) {
      if (ch === "{") depth++;
      else if (ch === "}") depth--;
      if (depth < 0) issues.push("unexpected '}'");
    }
    if (depth !== 0) issues.push("unclosed '{' block");
    return issues;
  }
  let parsed: any;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    return [`Invalid JSON in schema definition: ${(e as Error).message}`];
  }
  if (type === "AVRO") {
    if (typeof parsed === "string") {
      return AVRO_PRIMS.has(parsed) ? [] : [`Unknown Avro primitive '${parsed}'`];
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return ["Avro schema must be a JSON object or primitive string"];
    }
    if (typeof parsed.type !== "string") {
      return ['Avro schema object is missing required "type"'];
    }
    if (parsed.type === "record" && (!parsed.name || !Array.isArray(parsed.fields))) {
      return ['Avro record requires "name" and "fields" array'];
    }
    return [];
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return ["JSON Schema must be a JSON object"];
  }
  if (parsed.type !== undefined) {
    const types = Array.isArray(parsed.type) ? parsed.type : [parsed.type];
    if (!types.every((t: unknown) => typeof t === "string" && JSON_TYPES.has(t))) {
      return [`Invalid JSON Schema type: ${JSON.stringify(parsed.type)}`];
    }
  }
  return [];
}

function toCamel(s: string): string {
  return s.replace(/_([a-zA-Z])/g, (_m, c: string) => c.toUpperCase());
}

export function compileSchemaValidator(type: SchemaType, definition: string): CompiledValidator {
  const defIssues = validateSchemaDefinition(type, definition);
  if (defIssues.length > 0) {
    throw new SchemaValidation(`Invalid ${type} schema: ${defIssues.join("; ")}`, defIssues);
  }
  if (type === "PROTOBUF") {
    const fields = new Map<string, { type: string; required: boolean; repeated: boolean }>();
    for (const line of definition.split("\n")) {
      const m = /^(optional|required|repeated)?\s*([A-Za-z_][\w.]*)\s+([A-Za-z_]\w*)\s*=\s*(\d+)/.exec(
        line.trim(),
      );
      if (m) {
        const info = { type: m[2], required: m[1] === "required", repeated: m[1] === "repeated" };
        fields.set(m[3], info);
        fields.set(toCamel(m[3]), info);
      }
    }
    return (payload: Buffer) => {
      if (!payload || payload.length === 0) return ["Protobuf payload is empty"];
      const text = payload.toString("utf8").trim();
      if (!text.startsWith("{")) return [];
      let obj: any;
      try {
        obj = JSON.parse(text);
      } catch (e) {
        return [`Invalid JSON for Protobuf payload: ${(e as Error).message}`];
      }
      if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
        return ["Expected JSON object for Protobuf message"];
      }
      const issues: string[] = [];
      for (const [k, v] of Object.entries(obj)) {
        const f = fields.get(k);
        if (!f) {
          issues.push(`unknown field '${k}'`);
          continue;
        }
        if (v == null) continue;
        if (f.type.includes("int") && typeof v !== "number") {
          issues.push(`$.${k}: expected integer for ${f.type}`);
        } else if ((f.type === "float" || f.type === "double") && typeof v !== "number") {
          issues.push(`$.${k}: expected number for ${f.type}`);
        } else if (f.type === "bool" && typeof v !== "boolean") {
          issues.push(`$.${k}: expected boolean for bool`);
        } else if (f.type === "string" && typeof v !== "string") {
          issues.push(`$.${k}: expected string`);
        }
      }
      return issues;
    };
  }

  const parsedSchema = JSON.parse(definition);
  if (type === "AVRO") {
    return (payload: Buffer) => {
      if (!payload || payload.length === 0) return ["Avro payload is empty"];
      const text = payload.toString("utf8");
      let val: any;
      try {
        val = JSON.parse(text);
      } catch {
        if (parsedSchema === "string" || parsedSchema?.type === "string") {
          val = text;
        } else {
          return ["Payload is not valid JSON for Avro schema"];
        }
      }
      if (parsedSchema?.type === "record" && Array.isArray(parsedSchema.fields)) {
        if (!val || typeof val !== "object" || Array.isArray(val)) {
          return ["Expected JSON object for Avro record"];
        }
        const issues: string[] = [];
        const declared = new Set<string>();
        for (const f of parsedSchema.fields) {
          declared.add(f.name);
          const allowsNull =
            Array.isArray(f.type) && f.type.includes("null");
          if (!(f.name in val)) {
            if (!("default" in f) && !allowsNull) {
              issues.push(`missing required Avro field '${f.name}'`);
            }
          } else if (typeof f.type === "string") {
            const v = val[f.name];
            if ((f.type === "int" || f.type === "long") && (!Number.isInteger(v))) {
              issues.push(`$.${f.name}: expected integer for ${f.type}`);
            } else if ((f.type === "float" || f.type === "double") && typeof v !== "number") {
              issues.push(`$.${f.name}: expected number for ${f.type}`);
            } else if (f.type === "string" && typeof v !== "string") {
              issues.push(`$.${f.name}: expected string`);
            } else if (f.type === "boolean" && typeof v !== "boolean") {
              issues.push(`$.${f.name}: expected boolean`);
            }
          }
        }
        for (const k of Object.keys(val)) {
          if (!declared.has(k)) issues.push(`undeclared Avro field '${k}'`);
        }
        return issues;
      }
      return [];
    };
  }

  return (payload: Buffer) => {
    if (!payload || payload.length === 0) return ["JSON payload is empty"];
    const text = payload.toString("utf8");
    let val: any;
    try {
      val = JSON.parse(text);
    } catch {
      const types = Array.isArray(parsedSchema.type) ? parsedSchema.type : [parsedSchema.type];
      if (types.includes("string")) {
        val = text;
      } else {
        return ["Payload is not valid JSON"];
      }
    }
    const issues: string[] = [];
    validateJsonInstance(parsedSchema, val, "$", issues);
    return issues;
  };
}

function validateJsonInstance(schema: any, val: any, path: string, issues: string[]): void {
  if (!schema || typeof schema !== "object") return;
  if (schema.type !== undefined) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const ok = types.some((t: string) => {
      if (t === "object") return val !== null && typeof val === "object" && !Array.isArray(val);
      if (t === "array") return Array.isArray(val);
      if (t === "string") return typeof val === "string";
      if (t === "integer") return typeof val === "number" && Number.isInteger(val);
      if (t === "number") return typeof val === "number";
      if (t === "boolean") return typeof val === "boolean";
      if (t === "null") return val === null;
      return false;
    });
    if (!ok) {
      issues.push(`${path}: expected type ${JSON.stringify(schema.type)}`);
      return;
    }
  }
  if (typeof val === "string") {
    if (typeof schema.minLength === "number" && val.length < schema.minLength) {
      issues.push(`${path}: string length < minLength ${schema.minLength}`);
    }
    if (typeof schema.pattern === "string" && !new RegExp(schema.pattern).test(val)) {
      issues.push(`${path}: string does not match pattern '${schema.pattern}'`);
    }
  } else if (typeof val === "number") {
    if (typeof schema.minimum === "number" && val < schema.minimum) {
      issues.push(`${path}: value ${val} < minimum ${schema.minimum}`);
    }
    if (typeof schema.maximum === "number" && val > schema.maximum) {
      issues.push(`${path}: value ${val} > maximum ${schema.maximum}`);
    }
  } else if (val && typeof val === "object" && !Array.isArray(val)) {
    if (Array.isArray(schema.required)) {
      for (const req of schema.required) {
        if (!(req in val)) issues.push(`${path}: missing required field '${req}'`);
      }
    }
    const props = schema.properties && typeof schema.properties === "object" ? schema.properties : {};
    for (const [k, propSchema] of Object.entries(props)) {
      if (k in val) validateJsonInstance(propSchema, val[k], `${path}.${k}`, issues);
    }
    if (schema.additionalProperties === false) {
      for (const k of Object.keys(val)) {
        if (!(k in props)) issues.push(`${path}: unexpected additional property '${k}'`);
      }
    }
  }
}

function checkBackwardPair(type: SchemaType, readerDef: string, writerDef: string): string[] {
  if (type === "PROTOBUF") {
    const parseNums = (src: string) => {
      const map = new Map<number, { type: string; name: string; required: boolean }>();
      for (const line of src.split("\n")) {
        const m = /^(optional|required|repeated)?\s*([A-Za-z_][\w.]*)\s+([A-Za-z_]\w*)\s*=\s*(\d+)/.exec(line.trim());
        if (m) map.set(Number(m[4]), { type: m[2], name: m[3], required: m[1] === "required" });
      }
      return map;
    };
    const rMap = parseNums(readerDef);
    const wMap = parseNums(writerDef);
    const issues: string[] = [];
    for (const [num, rf] of rMap.entries()) {
      const wf = wMap.get(num);
      if (!wf) {
        if (rf.required) issues.push(`reader adds required field '${rf.name}' (#${num})`);
      } else {
        if (rf.type !== wf.type) {
          issues.push(`field #${num} changed type from '${wf.type}' to '${rf.type}'`);
        }
        if (rf.name !== wf.name) {
          issues.push(`field #${num} changed name from '${wf.name}' to '${rf.name}'`);
        }
      }
    }
    return issues;
  }
  const r = JSON.parse(readerDef);
  const w = JSON.parse(writerDef);
  const issues: string[] = [];
  if (type === "AVRO") {
    const rFields: any[] = Array.isArray(r?.fields) ? r.fields : [];
    const wFields: any[] = Array.isArray(w?.fields) ? w.fields : [];
    const wMap = new Map(wFields.map((f) => [f.name, f]));
    for (const rf of rFields) {
      const wf = wMap.get(rf.name);
      if (!wf) {
        const allowsNull = Array.isArray(rf.type) && rf.type.includes("null");
        if (!("default" in rf) && !allowsNull) {
          issues.push(`reader adds field '${rf.name}' without default`);
        }
      } else if (JSON.stringify(rf.type) !== JSON.stringify(wf.type)) {
        issues.push(`field '${rf.name}' changed type from ${JSON.stringify(wf.type)} to ${JSON.stringify(rf.type)}`);
      }
    }
    return issues;
  }
  const rReq = new Set<string>(Array.isArray(r?.required) ? r.required : []);
  const wReq = new Set<string>(Array.isArray(w?.required) ? w.required : []);
  const rProps = r?.properties ?? {};
  const wProps = w?.properties ?? {};
  for (const req of rReq) {
    if (!wReq.has(req) && !(rProps[req] && "default" in rProps[req])) {
      issues.push(`reader requires field '${req}' which is not required by writer and has no default`);
    }
  }
  for (const [k, rp] of Object.entries<any>(rProps)) {
    const wp = wProps[k];
    if (wp && rp?.type && wp?.type && rp.type !== wp.type && !(wp.type === "integer" && rp.type === "number")) {
      issues.push(`property '${k}' changed type from '${wp.type}' to '${rp.type}'`);
    }
  }
  return issues;
}

export function evaluateSchemaCompatibility(
  candidateType: SchemaType,
  candidateDef: string,
  targetVersions: SchemaVersion[],
  mode: SchemaCompatibility,
): SchemaCompatibilityResult {
  const defErrs = validateSchemaDefinition(candidateType, candidateDef);
  if (defErrs.length > 0) return { compatible: false, messages: defErrs };
  if (mode === "NONE" || targetVersions.length === 0) return { compatible: true, messages: [] };
  const isTransitive = mode.endsWith("_TRANSITIVE");
  const toCheck = isTransitive ? targetVersions : [targetVersions[targetVersions.length - 1]];
  const checksBack = mode.startsWith("BACKWARD") || mode.startsWith("FULL");
  const checksFwd = mode.startsWith("FORWARD") || mode.startsWith("FULL");
  const messages: string[] = [];
  for (const prev of toCheck) {
    if (prev.type !== candidateType) {
      messages.push(`schema type changed from ${prev.type} (v${prev.version}) to ${candidateType}`);
      continue;
    }
    if (checksBack) {
      for (const m of checkBackwardPair(candidateType, candidateDef, prev.definition)) {
        messages.push(`BACKWARD vs v${prev.version}: ${m}`);
      }
    }
    if (checksFwd) {
      for (const m of checkBackwardPair(candidateType, prev.definition, candidateDef)) {
        messages.push(`FORWARD vs v${prev.version}: ${m}`);
      }
    }
  }
  return { compatible: messages.length === 0, messages };
}

interface CachedSlot {
  entry: SchemaEntry | null;
  validate: CompiledValidator | null;
  expiresAtMs: number;
}

function parseEntryWire(fallbackKey: string, obj: any): SchemaEntry {
  const key = typeof obj?.key === "string" && obj.key ? obj.key : fallbackKey;
  const [rawRes, rawTarget] = key.includes("|") ? key.split("|") : [key, "message"];
  const resource = typeof obj?.resource === "string" && obj.resource ? obj.resource : rawRes;
  const target = normalizeSchemaTarget(typeof obj?.target === "string" ? obj.target : rawTarget);
  const type = normalizeSchemaType(typeof obj?.type === "string" ? obj.type : "JSON");
  const compatibility = (typeof obj?.compatibility === "string" ? obj.compatibility : "BACKWARD") as SchemaCompatibility;
  const version = typeof obj?.version === "number" && obj.version >= 1 ? obj.version : 1;
  const definition = typeof obj?.definition === "string" ? obj.definition : "";
  const updatedAtNs = BigInt(String(obj?.updated_at_ns ?? 0));
  const versions: SchemaVersion[] = Array.isArray(obj?.versions)
    ? obj.versions.map((v: any) => ({
        version: Number(v.version ?? 1),
        type: normalizeSchemaType(String(v.type ?? type)),
        definition: String(v.definition ?? ""),
        createdAtNs: BigInt(String(v.created_at_ns ?? 0)),
      }))
    : [{ version, type, definition, createdAtNs: updatedAtNs }];
  return {
    key: composeRegistryKey(resource, target),
    resource,
    target,
    type,
    compatibility,
    version,
    definition,
    updatedAtNs,
    versions,
    readOnly: Boolean(obj?.read_only),
    sourceKey: typeof obj?.source_key === "string" ? obj.source_key : null,
    sourceCluster: typeof obj?.source_cluster === "string" ? obj.source_cluster : null,
    linkedTargets: Array.isArray(obj?.linked_targets) ? obj.linked_targets.map(String) : [],
  };
}

function serializeEntryWire(entry: SchemaEntry): Record<string, unknown> {
  return {
    key: entry.key,
    resource: entry.resource,
    target: entry.target,
    type: entry.type,
    compatibility: entry.compatibility,
    version: entry.version,
    definition: entry.definition,
    updated_at_ns: entry.updatedAtNs.toString(),
    read_only: Boolean(entry.readOnly),
    ...(entry.sourceKey ? { source_key: entry.sourceKey } : {}),
    ...(entry.sourceCluster ? { source_cluster: entry.sourceCluster } : {}),
    linked_targets: entry.linkedTargets ?? [],
    versions: entry.versions.map((v) => ({
      version: v.version,
      type: v.type,
      definition: v.definition,
      created_at_ns: v.createdAtNs.toString(),
    })),
  };
}

export class SchemaRegistry {
  static readonly STORE_DOMAIN = SCHEMA_STORE_DOMAIN;
  static readonly STORE_NAME = SCHEMA_STORE_NAME;
  static readonly DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;

  private readonly slotCache = new Map<string, CachedSlot>();
  private readonly writerFeeds = new Map<string, string>();
  private cacheTtlMs = SchemaRegistry.DEFAULT_CACHE_TTL_MS;
  private _lookupCount = 0;
  private storeEnsured = false;

  constructor(private readonly transport: Transport) {}

  setCacheTtl(ttlMs: number): this {
    this.cacheTtlMs = Math.max(0, ttlMs);
    return this;
  }

  get lookupCount(): number {
    return this._lookupCount;
  }

  registerWriterFeed(writer: string, feed: string): void {
    if (writer && feed) this.writerFeeds.set(writer, feed);
  }

  feedForWriter(writer: string): string | undefined {
    return this.writerFeeds.get(writer);
  }

  refreshCache(): void {
    this.slotCache.clear();
  }

  async validateResourceWrite(
    resource: string,
    keyBytes: Buffer | null | undefined,
    messageBytes: Buffer | null | undefined,
  ): Promise<void> {
    if (!resource || !resource.trim() || isReservedSchemaStoreName(resource)) return;
    const norm = resource.trim().split("|")[0].trim();
    const violations: string[] = [];
    const [keySlot, msgSlot] = await Promise.all([
      this.resolveSlot(norm, "key"),
      this.resolveSlot(norm, "message"),
    ]);
    if (keySlot.entry && keySlot.validate && keyBytes && keyBytes.length > 0) {
      for (const err of keySlot.validate(keyBytes)) {
        violations.push(`${norm}|key (${keySlot.entry.type}): ${err}`);
      }
    }
    if (msgSlot.entry && msgSlot.validate) {
      const payload = messageBytes ?? Buffer.alloc(0);
      for (const err of msgSlot.validate(payload)) {
        violations.push(`${norm}|message (${msgSlot.entry.type}): ${err}`);
      }
    }
    if (violations.length > 0) {
      throw new SchemaValidation(
        `Schema validation failed for ${norm}: ${violations.join("; ")}`,
        violations,
      );
    }
  }

  private async resolveSlot(resource: string, target: SchemaTarget): Promise<CachedSlot> {
    const regKey = composeRegistryKey(resource, target);
    const now = Date.now();
    const existing = this.slotCache.get(regKey);
    if (existing && (existing.entry === null || now < existing.expiresAtMs)) {
      return existing;
    }
    const fetched = await this.lookupDirect(resource, target);
    if (!fetched) {
      const emptySlot: CachedSlot = { entry: null, validate: null, expiresAtMs: Number.MAX_SAFE_INTEGER };
      this.slotCache.set(regKey, emptySlot);
      return emptySlot;
    }
    if (
      existing?.entry &&
      existing.entry.version === fetched.version &&
      existing.entry.definition === fetched.definition
    ) {
      const renewed: CachedSlot = { entry: fetched, validate: existing.validate, expiresAtMs: now + this.cacheTtlMs };
      this.slotCache.set(regKey, renewed);
      return renewed;
    }
    const fresh: CachedSlot = {
      entry: fetched,
      validate: compileSchemaValidator(fetched.type, fetched.definition),
      expiresAtMs: now + this.cacheTtlMs,
    };
    this.slotCache.set(regKey, fresh);
    return fresh;
  }

  async getSchema(resource: string, target: SchemaTarget): Promise<SchemaEntry | null> {
    return this.lookupDirect(resource, normalizeSchemaTarget(target));
  }

  private async lookupDirect(resource: string, target: SchemaTarget): Promise<SchemaEntry | null> {
    const regKey = composeRegistryKey(resource, target);
    this._lookupCount += 1;
    try {
      const resp = (await this.transport.post("/v1/state-stores/lookup", {
        store: SCHEMA_STORE_NAME,
        key_base64: Buffer.from(regKey, "utf8").toString("base64"),
        strict: false,
      })) as Record<string, unknown>;
      if (resp?.found !== true || !resp?.payload_base64) return null;
      const decoded = Buffer.from(String(resp.payload_base64), "base64").toString("utf8");
      const parsed = JSON.parse(decoded);
      if (!parsed || typeof parsed !== "object" || typeof parsed.definition !== "string" || !parsed.definition.trim()) {
        return null;
      }
      return parseEntryWire(regKey, parsed);
    } catch {
      return null;
    }
  }

  async checkCompatibility(
    resource: string,
    definition: string,
    target: SchemaTarget,
    type: SchemaType,
  ): Promise<SchemaCompatibilityResult> {
    const normTarget = normalizeSchemaTarget(target);
    const normType = normalizeSchemaType(type);
    const existing = await this.lookupDirect(resource, normTarget);
    if (!existing) {
      const errs = validateSchemaDefinition(normType, definition);
      return { compatible: errs.length === 0, messages: errs };
    }
    return evaluateSchemaCompatibility(normType, definition, existing.versions, existing.compatibility);
  }

  async updateSchema(
    resource: string,
    definition: string,
    target: SchemaTarget,
    type: SchemaType,
    compatibility?: SchemaCompatibility,
  ): Promise<SchemaEntry> {
    if (isReservedSchemaStoreName(resource)) {
      throw new Error(`${SCHEMA_STORE_NAME} is an internal system store`);
    }
    const normTarget = normalizeSchemaTarget(target);
    const normType = normalizeSchemaType(type);
    const regKey = composeRegistryKey(resource, normTarget);
    const normRes = regKey.split("|")[0];
    const defIssues = validateSchemaDefinition(normType, definition);
    if (defIssues.length > 0) {
      throw new SchemaValidation(
        `Invalid ${normType} schema definition: ${defIssues.join("; ")}`,
        defIssues,
      );
    }
    await this.ensureStoreExists();
    const existing = await this.lookupDirect(normRes, normTarget);
    const nowNs = BigInt(Date.now()) * 1_000_000n;
    let nextEntry: SchemaEntry;
    if (!existing) {
      const mode = compatibility ?? "BACKWARD";
      const v1: SchemaVersion = { version: 1, type: normType, definition, createdAtNs: nowNs };
      nextEntry = {
        key: regKey,
        resource: normRes,
        target: normTarget,
        type: normType,
        compatibility: mode,
        version: 1,
        definition,
        updatedAtNs: nowNs,
        versions: [v1],
        readOnly: false,
        sourceKey: null,
        sourceCluster: null,
        linkedTargets: [],
      };
    } else {
      if (existing.readOnly) {
        throw new SchemaValidation(
          `${regKey} is a read-only linked schema (source: ${existing.sourceKey ?? existing.key}) — update the source schema instead`,
        );
      }
      const mode = compatibility ?? existing.compatibility;
      if (existing.type === normType && existing.definition.trim() === definition.trim()) {
        if (mode === existing.compatibility) {
          const validate = compileSchemaValidator(existing.type, existing.definition);
          this.slotCache.set(regKey, { entry: existing, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
          return existing;
        }
        nextEntry = { ...existing, compatibility: mode, updatedAtNs: nowNs };
      } else {
        const compat = evaluateSchemaCompatibility(normType, definition, existing.versions, mode);
        if (!compat.compatible) {
          throw new SchemaValidation(
            `Incompatible schema evolution for ${regKey} (${mode}): ${compat.messages.join("; ")}`,
            compat.messages,
          );
        }
        const nextVersion = existing.version + 1;
        const vNext: SchemaVersion = { version: nextVersion, type: normType, definition, createdAtNs: nowNs };
        nextEntry = {
          key: regKey,
          resource: normRes,
          target: normTarget,
          type: normType,
          compatibility: mode,
          version: nextVersion,
          definition,
          updatedAtNs: nowNs,
          versions: [...existing.versions, vNext],
          readOnly: false,
          sourceKey: null,
          sourceCluster: null,
          linkedTargets: existing.linkedTargets ?? [],
        };
      }
    }
    await this.putEntry(regKey, nextEntry);
    const validate = compileSchemaValidator(nextEntry.type, nextEntry.definition);
    this.slotCache.set(regKey, { entry: nextEntry, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
    for (const lk of nextEntry.linkedTargets ?? []) {
      const [lRes, lTarg] = lk.split("|");
      const copy: SchemaEntry = {
        ...nextEntry,
        key: lk,
        resource: lRes,
        target: normalizeSchemaTarget(lTarg),
        readOnly: true,
        sourceKey: nextEntry.key,
        sourceCluster: null,
        linkedTargets: [],
      };
      await this.putEntry(lk, copy);
      this.slotCache.set(lk, { entry: copy, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
    }
    return nextEntry;
  }

  async linkSchema(
    sourceResource: string,
    sourceTarget: SchemaTarget,
    linkedResource: string,
    linkedTarget: SchemaTarget,
  ): Promise<SchemaEntry> {
    if (isReservedSchemaStoreName(linkedResource)) {
      throw new Error(`${SCHEMA_STORE_NAME} is an internal system store`);
    }
    const sTarget = normalizeSchemaTarget(sourceTarget);
    const lTarget = normalizeSchemaTarget(linkedTarget);
    const sourceKey = composeRegistryKey(sourceResource, sTarget);
    const linkedKey = composeRegistryKey(linkedResource, lTarget);
    const owner = await this.lookupDirect(sourceResource, sTarget);
    if (!owner) throw new Refused(404, "schema.not_found", `Source schema not found: ${sourceKey}`);
    if (owner.readOnly) {
      throw new SchemaValidation(`${sourceKey} is itself a read-only linked schema`);
    }
    await this.ensureStoreExists();
    const nextTargets = [...new Set([...(owner.linkedTargets ?? []), linkedKey])];
    const nowNs = BigInt(Date.now()) * 1_000_000n;
    const updatedOwner: SchemaEntry = { ...owner, updatedAtNs: nowNs, linkedTargets: nextTargets };
    await this.putEntry(sourceKey, updatedOwner);
    const validate = compileSchemaValidator(updatedOwner.type, updatedOwner.definition);
    this.slotCache.set(sourceKey, { entry: updatedOwner, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
    const copy: SchemaEntry = {
      ...updatedOwner,
      key: linkedKey,
      resource: linkedKey.split("|")[0],
      target: lTarget,
      readOnly: true,
      sourceKey,
      sourceCluster: null,
      linkedTargets: [],
    };
    await this.putEntry(linkedKey, copy);
    this.slotCache.set(linkedKey, { entry: copy, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
    return copy;
  }

  async unlinkSchema(
    sourceResource: string,
    sourceTarget: SchemaTarget,
    linkedResource: string,
    linkedTarget: SchemaTarget,
  ): Promise<SchemaEntry> {
    const sTarget = normalizeSchemaTarget(sourceTarget);
    const lTarget = normalizeSchemaTarget(linkedTarget);
    const sourceKey = composeRegistryKey(sourceResource, sTarget);
    const linkedKey = composeRegistryKey(linkedResource, lTarget);
    const owner = await this.lookupDirect(sourceResource, sTarget);
    if (!owner) throw new Refused(404, "schema.not_found", `Source schema not found: ${sourceKey}`);
    await this.ensureStoreExists();
    const nextTargets = (owner.linkedTargets ?? []).filter((k) => k !== linkedKey);
    const updatedOwner: SchemaEntry = { ...owner, updatedAtNs: BigInt(Date.now()) * 1_000_000n, linkedTargets: nextTargets };
    await this.putEntry(sourceKey, updatedOwner);
    await this.transport.post("/v1/state-stores/mutate", {
      store: SCHEMA_STORE_NAME,
      mutation_id: randomUUID(),
      op: { kind: "delete_row", key_base64: Buffer.from(linkedKey, "utf8").toString("base64") },
    });
    this.slotCache.delete(linkedKey);
    return updatedOwner;
  }

  async replicateEntry(sourceEntry: SchemaEntry, sourceCluster: string): Promise<SchemaEntry> {
    await this.ensureStoreExists();
    const replica: SchemaEntry = {
      ...sourceEntry,
      updatedAtNs: BigInt(Date.now()) * 1_000_000n,
      readOnly: true,
      sourceKey: sourceEntry.sourceKey ?? sourceEntry.key,
      sourceCluster,
      linkedTargets: [],
    };
    await this.putEntry(replica.key, replica);
    const validate = compileSchemaValidator(replica.type, replica.definition);
    this.slotCache.set(replica.key, { entry: replica, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
    return replica;
  }

  async updateCompatibility(
    resource: string,
    target: SchemaTarget,
    compatibility: SchemaCompatibility,
  ): Promise<SchemaEntry> {
    const normTarget = normalizeSchemaTarget(target);
    const regKey = composeRegistryKey(resource, normTarget);
    const existing = await this.lookupDirect(resource, normTarget);
    if (!existing) throw new Refused(404, "schema.not_found", `Schema not found for ${regKey}`);
    const updated: SchemaEntry = { ...existing, compatibility, updatedAtNs: BigInt(Date.now()) * 1_000_000n };
    await this.putEntry(regKey, updated);
    const validate = compileSchemaValidator(updated.type, updated.definition);
    this.slotCache.set(regKey, { entry: updated, validate, expiresAtMs: Date.now() + this.cacheTtlMs });
    return updated;
  }

  private _unusedReturn(nextEntry: SchemaEntry) {
    return nextEntry;
  }

  async removeSchema(resource: string, target: SchemaTarget): Promise<boolean> {
    const normTarget = normalizeSchemaTarget(target);
    const regKey = composeRegistryKey(resource, normTarget);
    const existing = await this.lookupDirect(resource, normTarget);
    if (!existing) {
      this.slotCache.delete(regKey);
      return false;
    }
    await this.ensureStoreExists();
    await this.transport.post("/v1/state-stores/mutate", {
      store: SCHEMA_STORE_NAME,
      mutation_id: randomUUID(),
      op: { kind: "delete_row", key_base64: Buffer.from(regKey, "utf8").toString("base64") },
    });
    this.slotCache.delete(regKey);
    return true;
  }

  async listSchemas(): Promise<SchemaEntry[]> {
    const out: SchemaEntry[] = [];
    let afterKeyB64: string | undefined;
    for (;;) {
      let resp: any;
      try {
        resp = await this.transport.post("/v1/state-stores/iterate", {
          store: SCHEMA_STORE_NAME,
          limit: 256,
          ...(afterKeyB64 ? { after_key_base64: afterKeyB64 } : {}),
        });
      } catch (e) {
        if (e instanceof Refused || e instanceof Unavailable) {
          const msg = e.message.toLowerCase();
          if (msg.includes("does not exist") || msg.includes("not found") || msg.includes("no mutation-journal")) {
            return [];
          }
        }
        throw e;
      }
      const rows = Array.isArray(resp?.rows) ? resp.rows : [];
      if (rows.length === 0) break;
      for (const row of rows) {
        try {
          const key = Buffer.from(String(row.key_base64 ?? ""), "base64").toString("utf8");
          const payload = Buffer.from(String(row.payload_base64 ?? ""), "base64").toString("utf8");
          out.push(parseEntryWire(key, JSON.parse(payload)));
        } catch {
        }
      }
      if (!resp?.next_after_key_base64) break;
      afterKeyB64 = String(resp.next_after_key_base64);
    }
    return out.sort((a, b) => a.key.localeCompare(b.key));
  }

  private async putEntry(regKey: string, entry: SchemaEntry): Promise<void> {
    const payload = Buffer.from(JSON.stringify(serializeEntryWire(entry)), "utf8");
    await this.transport.post("/v1/state-stores/mutate", {
      store: SCHEMA_STORE_NAME,
      mutation_id: randomUUID(),
      op: {
        kind: "put_row",
        key_base64: Buffer.from(regKey, "utf8").toString("base64"),
        payload_base64: payload.toString("base64"),
      },
    });
  }

  private async ensureStoreExists(): Promise<void> {
    if (this.storeEnsured) return;
    try {
      await this.transport.post("/v1/admin/commands", {
        request_id: randomUUID(),
        commands: [{ command: "create_domain", name: SCHEMA_STORE_DOMAIN }],
      });
    } catch (e) {
      if (!(e instanceof WhitewaterError) || !e.message.toLowerCase().includes("exist")) throw e;
    }
    try {
      await this.transport.post("/v1/admin/commands", {
        request_id: randomUUID(),
        commands: [
          {
            command: "define_state_store",
            name: SCHEMA_STORE_NAME,
            source: { kind: "manual" },
            named_keys: [],
          },
        ],
      });
    } catch (e) {
      if (!(e instanceof WhitewaterError) || !e.message.toLowerCase().includes("exist")) throw e;
    }
    this.storeEnsured = true;
  }
}

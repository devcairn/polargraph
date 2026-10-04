import * as grpc from "@grpc/grpc-js";
import { promisify } from "util";

import {
  PolarGraphServiceClient as GrpcClient,
  ApplyChangesRequest,
  InsertRequest,
  QueryRequest,
  CypherQueryRequest,
  CypherWriteRequest,
  PropertyWriteMode,
  VarPattern,
  type LegacyStatus as PbLegacyStatus,
  type Vocabulary as PbVocabulary,
  type SearchVectorRequest,
  type BeginTransactionRequest,
  type CommitTransactionRequest,
  type RollbackTransactionRequest,
  type NodeId,
  type Triple,
  type PropertyTriple,
  type RelationTriple,
  type Term,
  type DatalogRule as PbDatalogRule,
  type EdgeProperty,
  type Value,
  type QueryStreamChunk,
} from "./proto/polargraph.js";

import type {
  BoundValue,
  Change,
  ChangeResult,
  ChangeSet,
  ConversionReport,
  LegacyStatus,
  Retraction,
  Vocabulary,
  ClientOptions,
  PatternSpec,
  QueryOptions,
  QueryResult,
  CypherOptions,
  CypherRow,
  WriteResult,
  SearchOptions,
  SearchResult,
  DatalogRule,
} from "./types.js";

// ── UUID helpers ──────────────────────────────────────────────────────────────

function uuidToBytes(uuid: string): Buffer {
  const hex = uuid.replace(/-/g, "");
  if (hex.length !== 32) throw new Error(`Invalid UUID: ${uuid}`);
  return Buffer.from(hex, "hex");
}

function bytesToUuid(bytes: Uint8Array): string {
  const hex = Buffer.from(bytes).toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}

function nodeIdProto(uuid: string): NodeId {
  return { bytes: uuidToBytes(uuid) };
}

function nodeIdString(n: NodeId): string {
  return bytesToUuid(n.bytes);
}

// ── Value encoding/decoding ───────────────────────────────────────────────────

function encodeValue(v: unknown): Value {
  if (v === null || v === undefined) return { nullVal: true };
  if (typeof v === "boolean") return { boolVal: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { intVal: v } : { floatVal: v };
  }
  if (typeof v === "string") return { textVal: v };
  if (v instanceof Buffer || v instanceof Uint8Array) {
    return { blobVal: Buffer.from(v) };
  }
  if (Array.isArray(v)) {
    return { vecVal: { values: v.map(Number) } };
  }
  throw new TypeError(`Cannot encode value of type ${typeof v}`);
}

/** A query row: node variables as UUID strings, value variables decoded. */
function queryRow(vars: { [k: string]: NodeId }, values: { [k: string]: Value }): QueryResult {
  const row: QueryResult = {};
  for (const [k, v] of Object.entries(vars)) {
    row[k] = nodeIdString(v);
  }
  for (const [k, v] of Object.entries(values)) {
    row[k] = decodeValue(v);
  }
  return row;
}

function decodeValue(v: Value): BoundValue {
  if (v.nullVal !== undefined) return null;
  if (v.boolVal !== undefined) return v.boolVal;
  if (v.intVal !== undefined) return v.intVal;
  if (v.floatVal !== undefined) return v.floatVal;
  if (v.textVal !== undefined) return v.textVal;
  if (v.blobVal !== undefined) return `<blob:${v.blobVal.length}B>`;
  if (v.vecVal !== undefined) return v.vecVal.values;
  if (v.langText !== undefined) return { "@value": v.langText.text, "@language": v.langText.lang };
  if (v.typed !== undefined) return { "@value": v.typed.lexical, "@type": v.typed.datatype };
  return null;
}

// ── Changeset / vocabulary helpers ───────────────────────────────────────────

/** The `rdf:type` predicate IRI: node types are relations to class IRIs. */
export const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

const WRITE_MODES: Record<NonNullable<Change["mode"]>, PropertyWriteMode> = {
  auto: PropertyWriteMode.PROPERTY_WRITE_MODE_AUTO,
  replace: PropertyWriteMode.PROPERTY_WRITE_MODE_REPLACE,
  add: PropertyWriteMode.PROPERTY_WRITE_MODE_ADD,
};

/** `node rdf:type <class>`; the server resolves `typeName` through its vocabulary. */
function typeTriple(nodeId: string, typeName: string): Triple {
  return {
    relation: {
      subject: nodeIdProto(nodeId),
      predicate: RDF_TYPE,
      objectIri: typeName,
      vtStart: 0,
      vtEnd: 0,
      properties: [],
    },
  };
}

function changeTriple(c: Change): Triple {
  if (c.value !== undefined) {
    return {
      property: {
        subject: nodeIdProto(c.subject),
        predicate: c.predicate,
        value: encodeValue(c.value),
        vtStart: 0,
        vtEnd: 0,
        mode: WRITE_MODES[c.mode ?? "auto"],
      },
    };
  }
  if (!c.object && !c.objectIri) {
    throw new Error(`change ${c.subject} ${c.predicate}: set value, object or objectIri`);
  }
  return {
    relation: {
      subject: nodeIdProto(c.subject),
      predicate: c.predicate,
      object: c.object ? nodeIdProto(c.object) : undefined,
      objectIri: c.objectIri ?? "",
      vtStart: 0,
      vtEnd: 0,
      properties: [],
    },
  };
}

function quadRef(r: Retraction) {
  return {
    subject: nodeIdProto(r.subject),
    predicate: r.predicate,
    graph: r.graph ?? "",
    ...(r.value !== undefined
      ? { value: encodeValue(r.value) }
      : { node: nodeIdProto(r.object ?? "") }),
  };
}

function legacyFromProto(l: PbLegacyStatus | undefined): LegacyStatus {
  return {
    conversionPending: l?.conversionPending ?? false,
    barePredicates: l?.barePredicates ?? [],
    typeLabels: Number(l?.typeLabels ?? 0),
    pendingMerges: l?.pendingMerges ?? [],
  };
}

function vocabularyFromProto(v: PbVocabulary): Vocabulary {
  return {
    base: v.base,
    prefixes: Object.fromEntries(v.prefixes.map((p) => [p.name, p.namespace])),
    legacy: legacyFromProto(v.legacy),
  };
}

let cypherWriteWarned = false;

// ── Pattern helpers ───────────────────────────────────────────────────────────

function makeTerm(slot: string | undefined): Term | undefined {
  if (!slot || slot === "_") return undefined;
  if (slot.startsWith("?")) return { var: slot.slice(1) };
  return { bound: nodeIdProto(slot) };
}

function patternProto(p: PatternSpec): VarPattern {
  const vp = VarPattern.fromPartial({ predicate: (p.p ?? "").replace(/^:/, "") });
  const s = makeTerm(p.s);
  const o = makeTerm(p.o);
  if (s) vp.subject = s;
  if (o) vp.object = o;
  return vp;
}

function ruleProto(r: DatalogRule): PbDatalogRule {
  return {
    headPredicate: r.headPredicate,
    headSubjectVar: r.headSubjectVar,
    headObjectVar: r.headObjectVar,
    body: r.body.map(patternProto),
  };
}

// ── promisify helper ──────────────────────────────────────────────────────────

type UnaryCallback<T> = (err: grpc.ServiceError | null, res: T) => void;
type UnaryFn<Req, Res> = (
  req: Req,
  meta: grpc.Metadata,
  opts: grpc.CallOptions,
  cb: UnaryCallback<Res>,
) => grpc.ClientUnaryCall;

function call<Req, Res>(
  fn: UnaryFn<Req, Res>,
  req: Req,
  meta: grpc.Metadata,
  opts: grpc.CallOptions,
): Promise<Res> {
  return new Promise((resolve, reject) => {
    fn(req, meta, opts, (err, res) => {
      if (err) reject(err);
      else resolve(res!);
    });
  });
}

// ── PolarGraphClient ──────────────────────────────────────────────────────────

/**
 * PolarGraph Node.js gRPC client.
 *
 * @example
 * ```ts
 * const client = new PolarGraphClient("localhost", 50051, { apiKey: "secret" });
 * await client.insertNode(aliceId, "Person", { name: "Alice" });
 * const rows = await client.query([{ s: aliceId, p: "knows", o: "?b" }]);
 * client.close();
 * ```
 */
export class PolarGraphClient {
  private readonly _grpc: GrpcClient;
  private readonly _meta: grpc.Metadata;
  private readonly _defaultOpts: grpc.CallOptions;

  /**
   * @param host    gRPC server hostname (default: `"localhost"`)
   * @param port    gRPC server port (default: `50051`)
   * @param options Optional configuration (apiKey, tlsCaCert, deadline)
   */
  constructor(
    host = "localhost",
    port = 50051,
    options: ClientOptions = {},
  ) {
    const { apiKey, tlsCaCert, deadline = 0 } = options;

    let creds: grpc.ChannelCredentials;
    if (tlsCaCert) {
      creds = grpc.credentials.createSsl(tlsCaCert);
    } else {
      creds = grpc.credentials.createInsecure();
    }

    this._grpc = new GrpcClient(`${host}:${port}`, creds);

    this._meta = new grpc.Metadata();
    if (apiKey) {
      this._meta.set("authorization", `Bearer ${apiKey}`);
    }

    this._defaultOpts = deadline > 0
      ? { deadline: Date.now() + deadline }
      : {};
  }

  private _opts(extraDeadline?: number): grpc.CallOptions {
    if (extraDeadline) return { deadline: Date.now() + extraDeadline };
    return this._defaultOpts;
  }

  private _unary<Req, Res>(
    fn: UnaryFn<Req, Res>,
    req: Req,
  ): Promise<Res> {
    return call(fn.bind(this._grpc) as UnaryFn<Req, Res>, req, this._meta, this._opts());
  }

  // ── Insert ──────────────────────────────────────────────────────────────────

  /**
   * Insert a node typed `rdf:type <typeName>` with optional scalar properties.
   * `typeName` is a bare name (under the server's vocabulary base), a
   * `prefix:local` name or a full IRI.
   */
  async insertNode(
    nodeId: string,
    typeName: string,
    properties: Record<string, unknown> = {},
  ): Promise<void> {
    const triples: Triple[] = [typeTriple(nodeId, typeName)];
    for (const [pred, val] of Object.entries(properties)) {
      triples.push({
        relation: undefined,
        property: {
          subject: nodeIdProto(nodeId),
          predicate: pred,
          value: encodeValue(val),
          vtStart: 0,
          vtEnd: 0,
        } as PropertyTriple,
      });
    }
    await this._unary(this._grpc.insert.bind(this._grpc), InsertRequest.fromPartial({ triples }));
  }

  /**
   * Insert a directed relation triple between two nodes.
   * `properties` become EdgeProperty entries on the relation triple.
   */
  async insertEdge(
    subject: string,
    predicate: string,
    object: string,
    properties: Record<string, unknown> = {},
  ): Promise<void> {
    const edgeProps: EdgeProperty[] = Object.entries(properties).map(
      ([name, val]) => ({ name, value: encodeValue(val) }),
    );
    const triple: Triple = {
      property: undefined,
      relation: {
        subject: nodeIdProto(subject),
        predicate,
        object: nodeIdProto(object),
        vtStart: 0,
        vtEnd: 0,
        properties: edgeProps,
      } as RelationTriple,
    };
    await this._unary(this._grpc.insert.bind(this._grpc), InsertRequest.fromPartial({ triples: [triple] }));
  }

  // ── Query ───────────────────────────────────────────────────────────────────

  /**
   * Execute a conjunctive pattern query and return all satisfying bindings.
   * Each result is a map from variable name (without `?`) to node UUID string.
   */
  async query(patterns: PatternSpec[], options: QueryOptions = {}): Promise<QueryResult[]> {
    const req = QueryRequest.fromPartial({
      patterns: patterns.map(patternProto),
      rules: (options.rules ?? []).map(ruleProto),
      asOfValidTime: options.asOfValidTime ?? 0,
      asOfTxTime: options.asOfTxTime ?? 0,
      txId: options.txId ?? "",
    });
    const resp = await this._unary(this._grpc.query.bind(this._grpc), req);
    return resp.bindings.map((b) => {
      return queryRow(b.vars, b.values);
    });
  }

  // ── Cypher ──────────────────────────────────────────────────────────────────

  /**
   * Execute a Cypher read query.
   * Node variables map to UUID strings; aggregate variables map to scalar values.
   */
  async cypher(query: string, options: CypherOptions = {}): Promise<CypherRow[]> {
    const req = CypherQueryRequest.fromPartial({
      cypher: query,
      vector: options.vector ?? [],
      ef: options.ef ?? 0,
      asOfValidTime: options.asOfValidTime ?? 0,
      asOfTxTime: options.asOfTxTime ?? 0,
      txId: options.txId ?? "",
      params: options.params ?? {},
    });
    const resp = await this._unary(this._grpc.cypherQuery.bind(this._grpc), req);
    return resp.rows.map((row) => {
      const out: CypherRow = {};
      for (const [k, v] of Object.entries(row.nodes)) {
        out[k] = nodeIdString(v);
      }
      for (const [k, v] of Object.entries(row.values)) {
        out[k] = decodeValue(v);
      }
      return out;
    });
  }

  /**
   * Execute a Cypher write statement (CREATE / MERGE / SET / DELETE).
   *
   * @deprecated Cypher writes are removed in the next server release; use
   * {@link applyChanges}.
   */
  async cypherWrite(query: string, txId?: string): Promise<WriteResult> {
    if (!cypherWriteWarned) {
      cypherWriteWarned = true;
      process.emitWarning(
        "cypherWrite is deprecated and will be removed in the next server release; use applyChanges",
        "DeprecationWarning",
      );
    }
    const req = CypherWriteRequest.fromPartial({ cypher: query, txId: txId ?? "" });
    const resp = await this._unary(this._grpc.cypherWrite.bind(this._grpc), req);
    return {
      createdNodeIds: resp.createdNodeIds.map((b) => bytesToUuid(b)),
      triplesWritten: Number(resp.triplesWritten),
      triplesDeleted: Number(resp.triplesDeleted),
    };
  }

  // ── Changesets ──────────────────────────────────────────────────────────────

  /**
   * Apply adds and retractions across graphs in one transaction (one commit,
   * one change-feed entry). Replaces the deprecated `cypherWrite`.
   *
   * ```ts
   * await client.applyChanges({ adds: { "": [
   *   { subject: id, predicate: RDF_TYPE, objectIri: "Person" },
   *   { subject: id, predicate: "name", value: "Alice" },
   * ] } });
   * ```
   */
  async applyChanges(cs: ChangeSet): Promise<ChangeResult> {
    const req = ApplyChangesRequest.fromPartial({
      adds: Object.entries(cs.adds ?? {}).map(([graph, changes]) => ({
        graph,
        triples: changes.map(changeTriple),
      })),
      retractions: (cs.retractions ?? []).map(quadRef),
      readTs: cs.readTs ?? 0,
      strict: cs.strict ?? false,
      iris: cs.iris ?? [],
    });
    const resp = await this._unary(this._grpc.applyChanges.bind(this._grpc), req);
    return {
      commitTs: Number(resp.commitTs),
      added: Number(resp.added),
      retracted: Number(resp.retracted),
      retractionsNotFound: Number(resp.retractionsNotFound),
      edgeIds: resp.edgeIds.map((b) => bytesToUuid(b)),
    };
  }

  // ── Vocabulary ──────────────────────────────────────────────────────────────

  /** Base IRI, prefixes and legacy-conversion status. */
  async getVocabulary(): Promise<Vocabulary> {
    return vocabularyFromProto(await this._unary(this._grpc.getVocabulary.bind(this._grpc), {}));
  }

  /** Set the base IRI for bare names (service calls, primary only). */
  async setVocabularyBase(base: string): Promise<Vocabulary> {
    const req = { base, userId: "" };
    return vocabularyFromProto(await this._unary(this._grpc.setVocabularyBase.bind(this._grpc), req));
  }

  /** Declare or re-point a prefix. */
  async putPrefix(name: string, namespace: string): Promise<Vocabulary> {
    const req = { name, namespace, userId: "" };
    return vocabularyFromProto(await this._unary(this._grpc.putPrefix.bind(this._grpc), req));
  }

  /** Remove a prefix (no-op if absent). */
  async removePrefix(name: string): Promise<Vocabulary> {
    const req = { name, userId: "" };
    return vocabularyFromProto(await this._unary(this._grpc.removePrefix.bind(this._grpc), req));
  }

  /** One-time conversion of pre-vocabulary data (idempotent, resumable). */
  async convertLegacyData(dryRun = false): Promise<ConversionReport> {
    const req = { dryRun, userId: "" };
    const r = await this._unary(this._grpc.convertLegacyData.bind(this._grpc), req);
    return {
      dryRun: r.dryRun,
      predicates: r.predicates.map((p) => ({
        from: p.from,
        to: p.to,
        merged: p.merged,
        quadsMoved: Number(p.quadsMoved),
      })),
      labelsConverted: Number(r.labelsConverted),
      legacy: legacyFromProto(r.legacy),
    };
  }

  // ── Vector search ────────────────────────────────────────────────────────────

  /**
   * Search for the k nearest neighbours of a query vector in a named HNSW space.
   */
  async searchVector(
    space: string,
    vector: number[],
    k: number,
    options: SearchOptions = {},
  ): Promise<SearchResult[]> {
    const req: SearchVectorRequest = {
      query: vector,
      k,
      space,
      ef: options.ef ?? 0,
      graphs: options.graphs ?? [],
    };
    const resp = await this._unary(this._grpc.searchVector.bind(this._grpc), req);
    return resp.results.map((r) => ({
      nodeId: nodeIdString(r.nodeId!),
      similarity: r.similarity,
    }));
  }

  // ── Streaming query ──────────────────────────────────────────────────────────

  /**
   * Stream query results via `QueryStream` RPC.
   * Yields one `QueryResult` per binding row, in chunks of up to 500.
   *
   * @example
   * ```ts
   * for await (const row of client.streamQuery([{ s: "?n", p: "knows", o: "?m" }])) {
   *   console.log(row);
   * }
   * ```
   */
  async *streamQuery(
    patterns: PatternSpec[],
    options: QueryOptions = {},
  ): AsyncIterable<QueryResult> {
    const req = QueryRequest.fromPartial({
      patterns: patterns.map(patternProto),
      rules: (options.rules ?? []).map(ruleProto),
      asOfValidTime: options.asOfValidTime ?? 0,
      asOfTxTime: options.asOfTxTime ?? 0,
      txId: options.txId ?? "",
    });
    const stream = this._grpc.queryStream(req, this._meta);
    for await (const chunk of streamToAsyncIterable<QueryStreamChunk>(stream)) {
      for (const result of chunk.results) {
        yield queryRow(result.vars, result.values);
      }
    }
  }

  // ── Transactions ─────────────────────────────────────────────────────────────

  /**
   * Open a new multi-RPC transaction. Returns an opaque `txId`.
   * Pass `txId` in `insertNode`, `insertEdge`, `query`, `cypher`, `cypherWrite` options.
   */
  async beginTx(): Promise<string> {
    const req: BeginTransactionRequest = {};
    const resp = await this._unary(this._grpc.beginTransaction.bind(this._grpc), req);
    return resp.txId;
  }

  /**
   * Commit an open transaction.
   * @returns the total number of triples written.
   * @throws grpc.ServiceError with code ABORTED on write-write conflict.
   * @throws grpc.ServiceError with code NOT_FOUND if txId is unknown or expired.
   */
  async commitTx(txId: string): Promise<number> {
    const req: CommitTransactionRequest = { txId };
    const resp = await this._unary(this._grpc.commitTransaction.bind(this._grpc), req);
    return Number(resp.triplesWritten);
  }

  /**
   * Roll back an open transaction, discarding all buffered writes.
   */
  async rollbackTx(txId: string): Promise<void> {
    const req: RollbackTransactionRequest = { txId };
    await this._unary(this._grpc.rollbackTransaction.bind(this._grpc), req);
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────────

  /** Close the underlying gRPC channel. */
  close(): void {
    this._grpc.close();
  }
}

// ── Factory shorthand ─────────────────────────────────────────────────────────

/**
 * Convenience factory: create a client from a URL string.
 *
 * @example
 * ```ts
 * const client = createClient("localhost:50051", "my-api-key");
 * ```
 */
export function createClient(url: string, apiKey?: string): PolarGraphClient {
  const withProto = url.replace(/^https?:\/\//, "");
  const [host, portStr] = withProto.split(":");
  const port = portStr ? parseInt(portStr, 10) : 50051;
  return new PolarGraphClient(host, port, { apiKey });
}

// ── Stream → AsyncIterable adapter ───────────────────────────────────────────

function streamToAsyncIterable<T>(
  stream: grpc.ClientReadableStream<T>,
): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      const queue: T[] = [];
      let done = false;
      let error: Error | null = null;
      let resolve: (() => void) | null = null;

      stream.on("data", (chunk: T) => {
        queue.push(chunk);
        resolve?.();
        resolve = null;
      });
      stream.on("end", () => {
        done = true;
        resolve?.();
        resolve = null;
      });
      stream.on("error", (err: Error) => {
        error = err;
        resolve?.();
        resolve = null;
      });

      return {
        async next() {
          while (queue.length === 0 && !done && !error) {
            await new Promise<void>((r) => { resolve = r; });
          }
          if (error) throw error;
          if (queue.length > 0) return { value: queue.shift()!, done: false };
          return { value: undefined as unknown as T, done: true };
        },
      };
    },
  };
}

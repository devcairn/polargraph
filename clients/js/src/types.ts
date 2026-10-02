/**
 * Higher-level TypeScript types for the PolarGraph client.
 * These wrap the raw proto types with ergonomic shapes for application code.
 */

// ── Patterns and queries ──────────────────────────────────────────────────────

/**
 * A triple pattern slot. One of:
 * - `"?varName"` — variable (binds on first match, constrains on subsequent)
 * - UUID string  — bound node ID
 * - `"_"` or omitted — wildcard (matches anything, not captured)
 */
export type Slot = string | undefined;

/** One triple pattern in a conjunctive query. */
export interface PatternSpec {
  s?: Slot;
  p?: string;
  o?: Slot;
}

/** A Datalog rule for recursive / transitive-closure queries. */
export interface DatalogRule {
  headPredicate: string;
  headSubjectVar: string;
  headObjectVar: string;
  body: PatternSpec[];
}

/** Options accepted by `query()` and `streamQuery()`. */
export interface QueryOptions {
  /** Optional list of Datalog rules for recursive queries. */
  rules?: DatalogRule[];
  /** Valid-time point-in-time filter (unix microseconds). 0 = no filter. */
  asOfValidTime?: number;
  /** Transaction-time snapshot override (unix microseconds). 0 = latest. */
  asOfTxTime?: number;
  /** Transaction ID to read from (write-your-own-reads). */
  txId?: string;
}

/** A language-tagged or typed RDF literal (JSON-LD value object). */
export type LiteralValue =
  | { "@value": string; "@language": string }
  | { "@value": string; "@type": string };

/** A property value bound to a query variable. */
export type BoundValue = string | number | boolean | null | number[] | LiteralValue;

/**
 * One satisfying variable binding: node variables map to UUID strings,
 * variables bound to property values (e.g. `?n` in `?p name ?n`) to the value.
 */
export type QueryResult = Record<string, BoundValue>;

// ── Cypher ────────────────────────────────────────────────────────────────────

/** Options for `cypher()` (read queries). */
export interface CypherOptions {
  /** Query embedding vector, required when Cypher contains VECTOR_NEAR(). */
  vector?: number[];
  /** HNSW exploration factor. 0 = server default. */
  ef?: number;
  /** Valid-time point-in-time filter (unix microseconds). */
  asOfValidTime?: number;
  /** Transaction-time snapshot override (unix microseconds). */
  asOfTxTime?: number;
  /** Transaction ID to read from. */
  txId?: string;
  /**
   * Named query parameters for `$param` substitution.
   * Keys are parameter names (without `$`); values are JSON-encoded strings
   * (e.g. `'"Alice"'` for a string value, `'42'` for an integer).
   */
  params?: Record<string, string>;
}

/**
 * One result row from a Cypher read query.
 * Node variables map to UUID strings; aggregate variables map to scalar values.
 */
export type CypherRow = Record<string, BoundValue>;

/** Result from a `cypherWrite()` call. */
export interface WriteResult {
  /** UUIDs of all nodes created by this statement. */
  createdNodeIds: string[];
  /** Total triples inserted. */
  triplesWritten: number;
  /** Total triples logically deleted (vt_end closed). */
  triplesDeleted: number;
}

// ── Changesets ────────────────────────────────────────────────────────────────

/**
 * One add in a {@link ChangeSet}: a property when `value` is set, otherwise a
 * relation to `object` (a node UUID) or `objectIri` (a full IRI,
 * `prefix:local` or a bare vocabulary name, resolved by the server).
 */
export interface Change {
  subject: string;
  predicate: string;
  object?: string;
  objectIri?: string;
  value?: unknown;
  /** Property write mode (default `"auto"`). */
  mode?: "auto" | "replace" | "add";
}

/** An exact live quad to close: a relation to `object`, or a property with `value`. */
export interface Retraction {
  subject: string;
  predicate: string;
  object?: string;
  value?: unknown;
  /** Graph IRI; `""` / omitted = default graph. */
  graph?: string;
}

/** Input to `applyChanges()`. */
export interface ChangeSet {
  /** Adds by graph IRI (`""` = default graph). */
  adds?: Record<string, Change[]>;
  retractions?: Retraction[];
  /** Abort if a touched quad was committed after this time (0 = no check). */
  readTs?: number;
  /** Fail if a retraction matches no live quad. */
  strict?: boolean;
  /** IRIs to record in the IRI dictionary. */
  iris?: string[];
}

/** Result from `applyChanges()`. */
export interface ChangeResult {
  commitTs: number;
  added: number;
  retracted: number;
  retractionsNotFound: number;
  edgeIds: string[];
}

// ── Vocabulary ────────────────────────────────────────────────────────────────

/** Pre-vocabulary data awaiting `convertLegacyData()`. */
export interface LegacyStatus {
  conversionPending: boolean;
  barePredicates: string[];
  typeLabels: number;
  pendingMerges: string[];
}

/** The server's base IRI for bare names and its prefixes. */
export interface Vocabulary {
  base: string;
  prefixes: Record<string, string>;
  legacy: LegacyStatus;
}

/** Result from `convertLegacyData()`. */
export interface ConversionReport {
  dryRun: boolean;
  predicates: { from: string; to: string; merged: boolean; quadsMoved: number }[];
  labelsConverted: number;
  legacy: LegacyStatus;
}

// ── Vector search ─────────────────────────────────────────────────────────────

/** Options for `searchVector()`. */
export interface SearchOptions {
  /** HNSW exploration factor. Higher = better recall, slower. */
  ef?: number;
}

/** One nearest-neighbour search result. */
export interface SearchResult {
  nodeId: string;
  /** Cosine similarity ∈ [-1, 1]; higher = more similar. */
  similarity: number;
}

// ── Client construction ───────────────────────────────────────────────────────

/** Options accepted by the `PolarGraphClient` constructor. */
export interface ClientOptions {
  /** Bearer API key forwarded on every RPC. */
  apiKey?: string;
  /** PEM CA certificate buffer for TLS verification. When set, TLS is enabled. */
  tlsCaCert?: Buffer;
  /** Default deadline in milliseconds for unary RPCs. 0 = no deadline. */
  deadline?: number;
}

/**
 * PolarGraph DB — Getting-Started Client Example
 * ================================================
 * Demonstrates the full feature surface of PolarGraph from TypeScript:
 *   - Schema registration (node types, edge types, vector spaces)
 *   - Triple insertion (properties + relations)
 *   - Batch vector upsert
 *   - Conjunctive graph queries
 *   - Transitive reachability
 *   - Filtered ANN search (by node type and by reachability)
 *
 * HOW TO RUN
 * ----------
 *   npx ts-node client-example.ts
 *
 * Make sure polargraphd is running first:
 *   cargo run -p polargraph-server -- --data-dir /tmp/pg-demo
 *   # or with Docker:
 *   docker compose up
 *
 * REQUIRED package.json DEPENDENCIES
 * ------------------------------------
 * {
 *   "dependencies": {
 *     "@grpc/grpc-js": "^1.10.0",
 *     "@grpc/proto-loader": "^0.7.0"
 *   },
 *   "devDependencies": {
 *     "typescript": "^5.0.0",
 *     "ts-node": "^10.9.0",
 *     "@types/node": "^20.0.0"
 *   }
 * }
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as path from "path";
import * as crypto from "crypto";

// ── Proto loading ──────────────────────────────────────────────────────────────

const PROTO_PATH = path.resolve(
  __dirname,
  "crates/polargraph-server/proto/polargraph.proto"
);

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,         // preserve field names exactly as in the .proto file
  longs: String,          // represent int64/uint64 as strings to avoid JS precision loss
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDef) as any;
const PolarGraphService = proto.polargraph.v1.PolarGraphService;

// ── Client setup ───────────────────────────────────────────────────────────────

// Change to "localhost:50051" or wherever your polargraphd is listening.
const TARGET = "localhost:50051";

// Wrap the callback-based gRPC client in a promisified helper so we can use
// async/await throughout.  The generic parameter T is the request type and R
// the response type — fill them in as needed for type safety in real projects.
function call<T, R>(
  client: any,
  method: string,
  request: T
): Promise<R> {
  return new Promise((resolve, reject) => {
    client[method](request, (err: grpc.ServiceError | null, res: R) => {
      if (err) reject(err);
      else resolve(res);
    });
  });
}

// ── UUID helpers ──────────────────────────────────────────────────────────────

/**
 * Generate a random 128-bit node ID as a 16-byte Buffer.
 *
 * PolarGraph uses UUID v7 (time-ordered) internally, but any 16 unique bytes
 * work as a node identifier from the client side.  For production use, prefer
 * a proper UUID v7 library (e.g. `uuidv7`) so IDs sort chronologically.
 */
function newNodeId(): Buffer {
  return crypto.randomBytes(16);
}

/**
 * Parse a standard UUID string ("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
 * into the 16-byte Buffer that PolarGraph expects.
 */
function uuidToBuffer(uuid: string): Buffer {
  const hex = uuid.replace(/-/g, "");
  return Buffer.from(hex, "hex");
}

/**
 * Format a 16-byte Buffer as a dashed UUID string for display.
 */
function bufferToUuid(buf: Buffer): string {
  const h = buf.toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// Wrap a raw buffer in the { bytes } envelope PolarGraph expects for NodeId.
function nodeId(buf: Buffer) {
  return { bytes: buf };
}

// ── Embedding helpers ─────────────────────────────────────────────────────────

const EMBEDDING_DIM = 1536; // must match the VectorSpaceDef dimensions below

/**
 * Generate a fake unit-normalised embedding vector for demo purposes.
 *
 * REPLACE THIS in a real application with actual embeddings from your model
 * provider, e.g.:
 *
 *   const openai = new OpenAI();
 *   const res = await openai.embeddings.create({
 *     model: "text-embedding-3-small",
 *     input: text,
 *   });
 *   return res.data[0].embedding;  // already unit-normalised
 */
function fakeEmbedding(dim: number = EMBEDDING_DIM): number[] {
  const raw = Array.from({ length: dim }, () => Math.random() * 2 - 1);
  const norm = Math.sqrt(raw.reduce((s, x) => s + x * x, 0));
  return raw.map((x) => x / norm);
}

// ── Triple constructors ───────────────────────────────────────────────────────

// vt_start = 0 and vt_end = "0" (the server interprets 0 as END_OF_TIME for
// vt_end, giving an open-ended "still current" fact).

function relationTriple(subject: Buffer, predicate: string, object: Buffer) {
  return {
    kind: "relation",
    relation: {
      subject: nodeId(subject),
      predicate,
      object: nodeId(object),
      vt_start: "0",
      vt_end: "0",
    },
  };
}

function textProp(subject: Buffer, predicate: string, text: string) {
  return {
    kind: "property",
    property: {
      subject: nodeId(subject),
      predicate,
      value: { kind: "text_val", text_val: text },
      vt_start: "0",
      vt_end: "0",
    },
  };
}

// ── Main demo ─────────────────────────────────────────────────────────────────

async function main() {
  const client = new PolarGraphService(
    TARGET,
    grpc.credentials.createInsecure()
  );

  console.log(`Connecting to PolarGraph at ${TARGET}...\n`);

  // ── 1. Register the Person node type ──────────────────────────────────────
  //
  // Declaring a schema is optional — PolarGraph is open-world and stores any
  // triple regardless.  Registering a schema enables:
  //   • validate_node() to check property maps before writing
  //   • dimension enforcement on insert_vector / batch_insert_vectors
  //   • richer introspection via list_node_types()

  console.log("── 1. Registering Person node type ──────────────────────────");
  await call(client, "registerNodeType", {
    definition: {
      type_name: "Person",
      fields: [
        { field_name: "name", kind: "text", required: true },
        { field_name: "bio",  kind: "text", required: false },
      ],
      // Attach a vector space: any insert_vector call targeting "Person"
      // will have its dimensionality validated against this value.
      vector_space: {
        space_name:      "Person",
        dimensions:      EMBEDDING_DIM,
        embedding_model: "text-embedding-3-small",
      },
    },
  });
  console.log("  ✓ Person type registered\n");

  // ── 2. Register the `knows` edge type ─────────────────────────────────────
  //
  // domain and range constrain which node types may appear as subject and
  // object.  Empty string means "unconstrained".

  console.log("── 2. Registering `knows` edge type ─────────────────────────");
  await call(client, "registerEdgeType", {
    definition: {
      predicate: "knows",
      domain:    "Person",
      range:     "Person",
      fields:    [],         // no edge-level properties required
    },
  });
  console.log("  ✓ `knows` edge type registered\n");

  // ── 3. Insert three Person nodes ──────────────────────────────────────────
  //
  // A node in PolarGraph has no explicit "create" call — it comes into
  // existence as soon as it appears as the subject of a triple.
  //
  // Convention: store the human-readable type as a `__type` property triple.
  // SearchVectorFiltered's NodeTypeFilter relies on exactly this predicate.

  console.log("── 3. Inserting three Person nodes ──────────────────────────");

  const aliceId = newNodeId();
  const bobId   = newNodeId();
  const carolId = newNodeId();

  console.log(`  alice  = ${bufferToUuid(aliceId)}`);
  console.log(`  bob    = ${bufferToUuid(bobId)}`);
  console.log(`  carol  = ${bufferToUuid(carolId)}`);

  const insertPeople = await call<any, any>(client, "insert", {
    triples: [
      // Alice
      textProp(aliceId, "__type", "Person"),
      textProp(aliceId, "name",   "Alice"),
      textProp(aliceId, "bio",    "Graph database enthusiast and ML researcher."),

      // Bob
      textProp(bobId, "__type", "Person"),
      textProp(bobId, "name",   "Bob"),
      textProp(bobId, "bio",    "Distributed systems engineer, coffee lover."),

      // Carol
      textProp(carolId, "__type", "Person"),
      textProp(carolId, "name",   "Carol"),
      textProp(carolId, "bio",    "Product designer with a fondness for type theory."),
    ],
  });
  console.log(`  ✓ Committed at ts=${insertPeople.commit_ts}\n`);

  // ── 4. Batch-insert embedding vectors ────────────────────────────────────
  //
  // Vectors live in a named HNSW space ("Person" here, matching the
  // VectorSpaceDef above).  Inserting into a registered space validates
  // that the vector length matches VectorSpaceDef.dimensions.
  //
  // In production, replace fakeEmbedding() with real model calls, e.g.:
  //   const vector = await openai.embeddings
  //     .create({ model: "text-embedding-3-small", input: node.bio })
  //     .then(r => r.data[0].embedding);

  console.log("── 4. Batch-inserting 1536-dim embedding vectors ────────────");
  const batchResp = await call<any, any>(client, "batchInsertVectors", {
    space: "Person",
    items: [
      { node_id: nodeId(aliceId), vector: fakeEmbedding() },
      { node_id: nodeId(bobId),   vector: fakeEmbedding() },
      { node_id: nodeId(carolId), vector: fakeEmbedding() },
    ],
  });

  if (batchResp.errors && batchResp.errors.length > 0) {
    console.error("  ✗ Batch insert errors:", batchResp.errors);
    process.exit(1);
  }
  console.log(`  ✓ Inserted ${batchResp.count_inserted} vectors into space "Person"\n`);

  // ── 5. Create `knows` edges ───────────────────────────────────────────────
  //
  // Alice knows Bob, and Bob knows Carol.  This gives us a two-hop chain
  // we can exercise with Reachable later.

  console.log("── 5. Creating `knows` edges ────────────────────────────────");
  const insertEdges = await call<any, any>(client, "insert", {
    triples: [
      relationTriple(aliceId, "knows", bobId),
      relationTriple(bobId,   "knows", carolId),
    ],
  });
  console.log(`  ✓ alice→bob and bob→carol committed at ts=${insertEdges.commit_ts}\n`);

  // ── 6. Query: who does Alice directly know? ───────────────────────────────
  //
  // VarPattern semantics:
  //   subject: bound to alice's NodeId
  //   predicate: "knows"
  //   object: variable "?person" — will be bound in each result Binding
  //
  // The response is a list of Binding objects: one per satisfying assignment.

  console.log("── 6. Query: people Alice directly knows ────────────────────");
  const queryResp = await call<any, any>(client, "query", {
    patterns: [
      {
        subject:   { kind: "bound", bound: nodeId(aliceId) },
        predicate: "knows",
        object:    { kind: "var",   var:   "person" },
      },
    ],
    snapshot_ts: "0",  // "0" → latest committed state
  });

  console.log(`  ${queryResp.bindings.length} result(s):`);
  for (const binding of queryResp.bindings) {
    const personBuf = Buffer.from(binding.vars.person.bytes);
    console.log(`    person = ${bufferToUuid(personBuf)}`);
  }
  console.log();

  // ── 7. Reachable: everyone Alice can reach via `knows` ────────────────────
  //
  // max_hops = 0 means unlimited depth — follows the full transitive closure.
  // Alice→Bob (hop 1) and Bob→Carol (hop 2) both appear.
  // The start node (Alice) is NOT included in the result.

  console.log("── 7. Reachable from Alice via `knows` (unlimited depth) ─────");
  const reachableResp = await call<any, any>(client, "reachable", {
    start:     nodeId(aliceId),
    predicate: "knows",
    max_hops:  0,
  });

  console.log(`  ${reachableResp.node_ids.length} node(s) reachable from Alice:`);
  for (const nid of reachableResp.node_ids) {
    console.log(`    ${bufferToUuid(Buffer.from(nid.bytes))}`);
  }
  console.log();

  // ── 8. SearchVectorFiltered — NodeTypeFilter ──────────────────────────────
  //
  // Finds the k nearest neighbors of a query vector, but restricts candidates
  // to nodes whose `__type` property is "Person".
  //
  // The server runs HNSW with a large candidate pool (k * 10) and then
  // post-filters to the matching set.  You get at most k results back.
  //
  // In production the query vector would come from the same embedding model
  // used during insertion.

  console.log("── 8. SearchVectorFiltered — NodeTypeFilter(Person) ─────────");
  const filteredByType = await call<any, any>(client, "searchVectorFiltered", {
    space: "Person",
    query: fakeEmbedding(),  // replace with a real query embedding
    k:     2,
    filter: {
      kind: "node_type_filter",
      node_type_filter: { type_name: "Person" },
    },
  });

  console.log(`  ${filteredByType.results.length} result(s):`);
  for (const r of filteredByType.results) {
    const id = bufferToUuid(Buffer.from(r.node_id.bytes));
    console.log(`    ${id}  similarity=${r.similarity.toFixed(4)}`);
  }
  console.log();

  // ── 9. SearchVectorFiltered — ReachabilityFilter ─────────────────────────
  //
  // Finds the k nearest neighbors among nodes reachable from Alice via
  // `knows`.  Useful for "recommend people similar to X within Alice's
  // extended network".
  //
  // max_hops = 0 → unlimited (Bob and Carol both qualify).

  console.log("── 9. SearchVectorFiltered — ReachabilityFilter from Alice ───");
  const filteredByReach = await call<any, any>(client, "searchVectorFiltered", {
    space: "Person",
    query: fakeEmbedding(),  // replace with a real query embedding
    k:     2,
    filter: {
      kind: "reachability_filter",
      reachability_filter: {
        from_node:  nodeId(aliceId),
        predicate:  "knows",
        max_hops:   0,
      },
    },
  });

  console.log(`  ${filteredByReach.results.length} result(s) within Alice's reach:`);
  for (const r of filteredByReach.results) {
    const id = bufferToUuid(Buffer.from(r.node_id.bytes));
    console.log(`    ${id}  similarity=${r.similarity.toFixed(4)}`);
  }
  console.log();

  // ── Done ──────────────────────────────────────────────────────────────────

  console.log("── All steps complete ───────────────────────────────────────");
  client.close();
}

main().catch((err) => {
  console.error("Fatal error:", err.message ?? err);
  process.exit(1);
});

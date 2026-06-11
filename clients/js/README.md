# @polargraph/client

JavaScript/TypeScript client SDK for the [PolarGraph DB Engine](../../README.md) gRPC API.

## Installation

```bash
npm install @polargraph/client
```

## Quickstart

```typescript
import { randomUUID } from "crypto";
import { PolarGraphClient } from "@polargraph/client";

const client = new PolarGraphClient("localhost", 50051, { apiKey: "secret" });

const aliceId = randomUUID();
const bobId   = randomUUID();

// Insert nodes
await client.insertNode(aliceId, "Person", { name: "Alice", age: 30 });
await client.insertNode(bobId,   "Person", { name: "Bob",   age: 25 });

// Insert an edge
await client.insertEdge(aliceId, "knows", bobId, { since: 2024 });

// Pattern query
const rows = await client.query([
  { s: aliceId, p: "knows", o: "?friend" },
]);
console.log(rows); // [{ friend: "<bobId>" }]

// Cypher read
const cyRows = await client.cypher(
  "MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b LIMIT 10"
);

// Cypher write
const result = await client.cypherWrite('CREATE (c:Company {name: "Acme"})');
console.log(result.createdNodeIds);

client.close();
```

## Factory shorthand

```typescript
import { createClient } from "@polargraph/client";

const client = createClient("localhost:50051", "my-api-key");
```

## Streaming queries

Large result sets can be streamed one binding at a time without loading
everything into memory:

```typescript
for await (const row of client.streamQuery([
  { s: "?n", p: "__type", o: "?t" },
])) {
  console.log(row.n, row.t);
}
```

## Vector search

```typescript
const results = await client.searchVector("embeddings", [0.1, 0.2, 0.3], 5);
// [{ nodeId: "...", similarity: 0.98 }, ...]
```

## Wire transactions

```typescript
const txId = await client.beginTx();
try {
  await client.insertNode(randomUUID(), "Person", { name: "Dave" });
  await client.insertEdge(aliceId, "knows", bobId);
  const written = await client.commitTx(txId);
  console.log(`Committed ${written} triples`);
} catch (err) {
  await client.rollbackTx(txId);
  throw err;
}
```

## TLS

```typescript
import { readFileSync } from "fs";

const client = new PolarGraphClient("db.example.com", 50051, {
  apiKey: process.env.POLAR_API_KEY,
  tlsCaCert: readFileSync("/path/to/ca.pem"),
});
```

## API reference

### `new PolarGraphClient(host?, port?, options?)`

| Option | Type | Description |
|--------|------|-------------|
| `apiKey` | `string` | Bearer token forwarded on every RPC |
| `tlsCaCert` | `Buffer` | PEM CA cert — enables TLS when set |
| `deadline` | `number` | Default deadline in ms for unary RPCs (0 = none) |

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `insertNode(nodeId, typeName, props?)` | `Promise<void>` | Insert node with `__type` and optional property triples |
| `insertEdge(subject, predicate, object, props?)` | `Promise<void>` | Insert directed relation triple |
| `query(patterns, options?)` | `Promise<QueryResult[]>` | Conjunctive pattern query |
| `cypher(query, options?)` | `Promise<CypherRow[]>` | Cypher read query |
| `cypherWrite(query, txId?)` | `Promise<WriteResult>` | Cypher write (CREATE/MERGE/SET/DELETE) |
| `searchVector(space, vector, k, options?)` | `Promise<SearchResult[]>` | k-NN vector search |
| `streamQuery(patterns, options?)` | `AsyncIterable<QueryResult>` | Streaming pattern query |
| `beginTx()` | `Promise<string>` | Open a wire transaction, returns `txId` |
| `commitTx(txId)` | `Promise<number>` | Commit transaction, returns triples written |
| `rollbackTx(txId)` | `Promise<void>` | Roll back transaction |
| `close()` | `void` | Close the gRPC channel |

### `PatternSpec`

```typescript
{ s?: string; p?: string; o?: string }
```

A slot can be:
- `"?varName"` — variable (captured in result)
- UUID string — bound node ID (exact match)
- `"_"` or omitted — wildcard

### `QueryOptions`

```typescript
{
  rules?: DatalogRule[];       // recursive Datalog rules
  asOfValidTime?: number;      // unix µs valid-time snapshot
  asOfTxTime?: number;         // unix µs tx-time snapshot
  txId?: string;               // open transaction ID
}
```

## Regenerating proto types

After updating `polargraph.proto`, regenerate the committed TypeScript stubs:

```bash
cd clients/js
scripts/regen_proto.sh
```

Requires `protoc` (`brew install protobuf`) and `ts-proto` (already a dev dependency).

## Connection pooling

The underlying `@grpc/grpc-js` channel manages connection pooling and
multiplexing automatically. Create one `PolarGraphClient` per process and
reuse it — there is no need to create a pool manually.

For custom tuning, instantiate a grpc channel directly and pass its
`ChannelCredentials` to `new PolarGraphClient()`.

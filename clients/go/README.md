# polargraph-go — Go SDK

Official Go client for the [PolarGraph DB Engine](https://github.com/devcairn/polargraph).

## Installation

```bash
go get github.com/devcairn/polargraph-go
```

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/devcairn/polargraph-go/polargraph"
    "github.com/google/uuid"
)

func main() {
    client, err := polargraph.New("localhost:50051",
        polargraph.WithAPIKey("secret"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    aliceID := "01234567-89ab-cdef-0123-456789abcdef"

    // Insert a node
    client.InsertNode(ctx, aliceID, "Person", map[string]interface{}{
        "name": "Alice",
        "age":  int64(30),
    })

    // Insert an edge
    bobID := "fedcba98-7654-3210-fedc-ba9876543210"
    client.InsertEdge(ctx, aliceID, "knows", bobID, map[string]interface{}{
        "since": int64(2023),
    })

    // Pattern query
    rows, err := client.Query(ctx, polargraph.QueryRequest{
        Patterns: []polargraph.Pattern{
            {S: aliceID, P: "knows", O: "?friend"},
        },
    })
    fmt.Println(rows, err)

    // Cypher query
    cyRows, err := client.Cypher(ctx,
        "MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b LIMIT 10",
    )
    fmt.Println(cyRows, err)

    // Writes: one atomic changeset (CypherWrite is deprecated)
    acmeID := uuid.NewString()
    result, err := client.ApplyChanges(ctx, polargraph.ChangeSet{Adds: map[string][]polargraph.Change{"": {
        {Subject: acmeID, Predicate: polargraph.RDFType, ObjectIRI: "Company"},
        {Subject: acmeID, Predicate: "name", Value: "Acme"},
    }}})
    fmt.Println(result, err)

    // Vector search
    results, err := client.SearchVector(ctx, "embeddings", []float32{0.1, 0.2, 0.3}, 5)
    fmt.Println(results, err)
}
```

## TLS

```go
client, err := polargraph.New("myserver:50051",
    polargraph.WithTLSCA("/path/to/ca.pem"),
    polargraph.WithAPIKey("secret"),
)
```

## Pattern format

| Value | Meaning |
|-------|---------|
| `""` or `"_"` | Wildcard — matches anything |
| `"?varname"` | Variable — binds on first match, constrains later |
| `"<uuid>"` | Bound node ID |
| `":predname"` | Predicate (leading colon stripped automatically) |

## Wire transactions

```go
txID, err := client.BeginTransaction(ctx)
if err != nil {
    log.Fatal(err)
}

_, err = client.InsertNode(ctx, nodeID, "Event",
    map[string]interface{}{"name": "Deploy"},
    polargraph.WithTxID(txID),
)
if err != nil {
    client.RollbackTransaction(ctx, txID)
    log.Fatal(err)
}

result, err := client.CommitTransaction(ctx, txID)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("committed %d triples at ts=%d\n", result.TriplesWritten, result.CommitTs)
```

## Available methods

| Method | Description |
|--------|-------------|
| `InsertNode(ctx, nodeID, typeName, props, opts...)` | Insert a node typed `rdf:type <typeName>` (bare name, `prefix:local` or IRI, resolved by the server's vocabulary) and properties |
| `InsertEdge(ctx, subject, predicate, object, props, opts...)` | Insert a relation triple |
| `Query(ctx, QueryRequest) ([]Bindings, error)` | Conjunctive pattern query (node variables only) |
| `QueryRows(ctx, QueryRequest) ([]Row, error)` | Same, with variables bound to property values (`?n` in `?p name ?n`) in `Row.Values` |
| `Cypher(ctx, query, opts...) ([]CypherRow, error)` | Cypher read query |
| `CypherWrite(ctx, query, opts...) (WriteResult, error)` | **Deprecated** (removed in the next server release) — use `ApplyChanges` |
| `ApplyChanges(ctx, ChangeSet) (*ChangeResult, error)` | Atomic changeset: adds per graph (`Change{Subject, Predicate, Object \| ObjectIRI \| Value, Mode}`), exact `Retractions`, `ReadTS`, `Strict` |
| `GetVocabulary` / `SetVocabularyBase` / `PutPrefix` / `RemovePrefix` | Runtime vocabulary (base IRI for bare names, prefixes) |
| `ConvertLegacyData(ctx, dryRun) (*ConversionReport, error)` | One-time conversion of pre-vocabulary data |
| `InsertVector(ctx, nodeID, space, vector, opts...)` | Insert embedding into named HNSW space |
| `SearchVector(ctx, space, vector, k, opts...) ([]SearchResult, error)` | k-NN search |
| `BeginTransaction(ctx) (string, error)` | Open a wire transaction; returns `txID` |
| `CommitTransaction(ctx, txID) (CommitResult, error)` | Commit; returns commit timestamp and triples written |
| `RollbackTransaction(ctx, txID) error` | Discard a transaction |
| `ShowIndexes(ctx) (IndexStats, error)` | Column-family statistics |
| `ShowStats(ctx) (ServerStats, error)` | Server internals snapshot |
| `Close()` | Close the gRPC channel |

## Datalog rules

```go
rows, err := client.Query(ctx, polargraph.QueryRequest{
    Patterns: []polargraph.Pattern{
        {S: "?a", P: "reachable", O: "?b"},
    },
    Rules: []polargraph.DatalogRule{
        {
            HeadPredicate:  "reachable",
            HeadSubjectVar: "a",
            HeadObjectVar:  "b",
            Body: []polargraph.Pattern{
                {S: "?a", P: "knows", O: "?b"},
            },
        },
    },
})
```

## Regenerating proto stubs

Maintainers only:

```bash
../../scripts/regen_proto.sh
```

## Running tests

```bash
go test ./polargraph/...
```

Integration tests (require a running `polargraphd`):

```bash
POLARGRAPH_TEST_ADDR=localhost:50051 go test -tags integration ./polargraph/...
```

## License

MIT

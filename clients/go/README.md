# polargraph-go — Go SDK

Official Go client for the [PolarGraph DB Engine](https://github.com/polarops/polargraph).

## Installation

```bash
go get github.com/polarops/polargraph-go
```

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/polarops/polargraph-go/polargraph"
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

    // Cypher write
    result, err := client.CypherWrite(ctx, `CREATE (c:Company {name: "Acme"})`)
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

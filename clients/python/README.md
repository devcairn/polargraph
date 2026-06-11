# polargraph-client — Python SDK

Official Python client for the [PolarGraph DB Engine](https://github.com/polarops/polargraph).

## Installation

```bash
pip install polargraph-client
```

## Quickstart

```python
import uuid
from polargraph import PolarGraphClient

alice_id = str(uuid.uuid4())
bob_id   = str(uuid.uuid4())

with PolarGraphClient("localhost", 50051, api_key="secret") as client:
    # Insert nodes
    client.insert_node(alice_id, "Person", name="Alice", age=30)
    client.insert_node(bob_id,   "Person", name="Bob",   age=25)

    # Insert an edge
    client.insert_edge(alice_id, "knows", bob_id, since=2023)

    # Pattern query
    results = client.query([{"s": alice_id, "p": "knows", "o": "?friend"}])
    print(results)

    # Cypher query
    rows = client.cypher("MATCH (a:Person)-[:knows]->(b) RETURN a, b LIMIT 10")
    print(rows)

    # Cypher write
    out = client.cypher_write('CREATE (c:Company {name: "Acme"})')
    print(out["created_node_ids"])

    # Vector insert + search
    client.insert_vector(alice_id, [0.1, 0.2, 0.3], space="embeddings")
    neighbors = client.search_vector("embeddings", [0.1, 0.2, 0.3], k=5)
    print(neighbors)
```

## Async client

```python
import asyncio
from polargraph import AsyncPolarGraphClient

async def main():
    async with AsyncPolarGraphClient("localhost", 50051) as client:
        rows = await client.cypher("MATCH (a:Person) RETURN a LIMIT 5")
        print(rows)

        # Async streaming
        async for row in client.stream_query([{"s": "?n", "p": "__type", "o": None}]):
            print(row)

asyncio.run(main())
```

## TLS

```python
client = PolarGraphClient("myserver", 50051, tls_ca_cert="/path/to/ca.pem")
```

## Pattern format

Patterns are dicts with keys `s`, `p`, `o` (or `subject`, `predicate`, `object`):

| Value | Meaning |
|-------|---------|
| `"?varname"` | Variable — binds on first match, constrains later |
| `"<uuid>"` | Bound node ID |
| `None` or `"_"` | Wildcard — matches anything |

## Streaming queries

```python
for row in client.stream_query([{"s": "?a", "p": "knows", "o": "?b"}]):
    print(row)
```

## Regenerating proto stubs

Maintainers only — consumers do not need this:

```bash
./scripts/regen_proto.sh
```

## License

MIT

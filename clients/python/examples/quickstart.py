"""PolarGraph Python SDK — quickstart example.

Run against a local polargraphd:
    python examples/quickstart.py
"""

import uuid
from polargraph import PolarGraphClient

alice_id = str(uuid.uuid4())
bob_id   = str(uuid.uuid4())
carol_id = str(uuid.uuid4())

with PolarGraphClient("localhost", 50051) as client:
    # Insert three nodes
    client.insert_node(alice_id, "Person", name="Alice", age=30)
    client.insert_node(bob_id,   "Person", name="Bob",   age=25)
    client.insert_node(carol_id, "Person", name="Carol", age=28)

    # Insert two edges
    client.insert_edge(alice_id, "knows", bob_id,   since=2020)
    client.insert_edge(bob_id,   "knows", carol_id, since=2022)

    # Pattern query — who does Alice know?
    results = client.query([
        {"s": alice_id, "p": "knows", "o": "?friend"},
    ])
    print("Alice knows:")
    for row in results:
        print(" ", row)

    # Cypher query
    rows = client.cypher(
        'MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b LIMIT 10'
    )
    print("\nCypher MATCH knows:")
    for row in rows:
        print(" ", row)

    # Streaming query
    print("\nStreaming all Person nodes:")
    for row in client.stream_query([{"s": "?n", "p": "__type", "o": None}]):
        print(" ", row)

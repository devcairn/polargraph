"""PolarGraph Python SDK — async quickstart example.

Run against a local polargraphd:
    python examples/async_example.py
"""

import asyncio
import uuid
from polargraph import AsyncPolarGraphClient

alice_id = str(uuid.uuid4())
bob_id   = str(uuid.uuid4())


async def main() -> None:
    async with AsyncPolarGraphClient("localhost", 50051) as client:
        await client.insert_node(alice_id, "Person", name="Alice")
        await client.insert_node(bob_id,   "Person", name="Bob")
        await client.insert_edge(alice_id, "knows", bob_id)

        rows = await client.query([{"s": "?a", "p": "knows", "o": "?b"}])
        print("knows bindings:", rows)

        rows = await client.cypher("MATCH (a:Person) RETURN a LIMIT 5")
        print("Cypher Person nodes:", rows)

        print("Streaming:")
        async for row in client.stream_query([{"s": "?n", "p": "__type", "o": None}]):
            print(" ", row)


if __name__ == "__main__":
    asyncio.run(main())

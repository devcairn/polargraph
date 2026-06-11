/**
 * PolarGraph JavaScript SDK — quickstart example.
 *
 * Run against a local polargraphd:
 *   npx ts-node examples/quickstart.ts
 *
 * Or compile first:
 *   npm run build && node dist/cjs/examples/quickstart.js
 */

import { randomUUID } from "crypto";
import { PolarGraphClient } from "../src/index.js";

async function main() {
  const client = new PolarGraphClient("localhost", 50051);

  const aliceId = randomUUID();
  const bobId   = randomUUID();
  const carolId = randomUUID();

  // Insert three Person nodes
  await client.insertNode(aliceId, "Person", { name: "Alice", age: 30 });
  await client.insertNode(bobId,   "Person", { name: "Bob",   age: 25 });
  await client.insertNode(carolId, "Person", { name: "Carol", age: 28 });
  console.log("Inserted 3 nodes");

  // Insert two edges
  await client.insertEdge(aliceId, "knows", bobId,   { since: 2020 });
  await client.insertEdge(bobId,   "knows", carolId, { since: 2022 });
  console.log("Inserted 2 edges");

  // Pattern query — who does Alice know?
  const friends = await client.query([
    { s: aliceId, p: "knows", o: "?friend" },
  ]);
  console.log("\nAlice knows (pattern query):");
  for (const row of friends) {
    console.log("  friend =", row.friend);
  }

  // Cypher read query
  const cypherRows = await client.cypher(
    "MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b LIMIT 10"
  );
  console.log("\nCypher MATCH knows:");
  for (const row of cypherRows) {
    console.log(" ", row);
  }

  // Cypher write
  const result = await client.cypherWrite(
    'CREATE (c:Company {name: "Acme"})'
  );
  console.log("\nCreated company node IDs:", result.createdNodeIds);
  console.log("Triples written:", result.triplesWritten);

  // Wire transaction example
  const txId = await client.beginTx();
  await client.insertNode(randomUUID(), "Person", { name: "Dave" });
  const triplesWritten = await client.commitTx(txId);
  console.log(`\nTransaction committed (${triplesWritten} triples)`);

  client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

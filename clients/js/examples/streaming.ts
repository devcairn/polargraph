/**
 * PolarGraph JavaScript SDK — streaming query example.
 *
 * Demonstrates `streamQuery` using a `for await` loop.
 * The QueryStream RPC delivers results in chunks of up to 500 bindings;
 * the client presents them as a flat AsyncIterable<QueryResult>.
 *
 * Run against a local polargraphd:
 *   npx ts-node examples/streaming.ts
 */

import { randomUUID } from "crypto";
import { PolarGraphClient } from "../src/index.js";

async function main() {
  const client = new PolarGraphClient("localhost", 50051);

  // Seed some data
  const nodeIds: string[] = [];
  for (let i = 0; i < 20; i++) {
    const id = randomUUID();
    nodeIds.push(id);
    await client.insertNode(id, "Item", { index: i, label: `item-${i}` });
  }
  console.log(`Inserted ${nodeIds.length} Item nodes\n`);

  // Stream all Item nodes using QueryStream RPC
  console.log("Streaming all Item nodes via streamQuery:");
  let count = 0;
  for await (const row of client.streamQuery([
    { s: "?node", p: "__type", o: "?type" },
  ])) {
    count++;
    if (count <= 5) {
      console.log(`  [${count}] node=${row.node}  type=${row.type}`);
    }
  }
  console.log(`  ... (${count} total rows streamed)\n`);

  // Demonstrate recursive streaming with DatalogRules
  // Build a small knows chain: 0 → 1 → 2 → 3
  for (let i = 0; i < nodeIds.length - 1; i++) {
    await client.insertEdge(nodeIds[i], "knows", nodeIds[i + 1]);
  }

  console.log("Streaming transitive closure via rules + streamQuery:");
  let reachCount = 0;
  for await (const row of client.streamQuery(
    [{ s: "?x", p: "reachable", o: "?y" }],
    {
      rules: [
        {
          headPredicate: "reachable",
          headSubjectVar: "x",
          headObjectVar: "y",
          body: [{ s: "?x", p: "knows", o: "?y" }],
        },
        {
          headPredicate: "reachable",
          headSubjectVar: "x",
          headObjectVar: "z",
          body: [
            { s: "?x", p: "knows", o: "?y" },
            { s: "?y", p: "reachable", o: "?z" },
          ],
        },
      ],
    }
  )) {
    reachCount++;
    if (reachCount <= 5) {
      console.log(`  [${reachCount}] x=${row.x?.slice(0, 8)}…  y=${row.y?.slice(0, 8)}…`);
    }
  }
  console.log(`  ... (${reachCount} reachability pairs total)\n`);

  client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Quickstart example for the PolarGraph Go SDK.
// Run against a local polargraphd: go run ./examples/quickstart/
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/polarops/polargraph-go/polargraph"
	"github.com/google/uuid"
)

func mustUUID() string { return uuid.New().String() }

func main() {
	client, err := polargraph.New("localhost:50051")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	aliceID := mustUUID()
	bobID   := mustUUID()
	carolID := mustUUID()

	// Insert three nodes
	for _, n := range []struct{ id, name string }{
		{aliceID, "Alice"},
		{bobID, "Bob"},
		{carolID, "Carol"},
	} {
		if err := client.InsertNode(ctx, n.id, "Person", map[string]interface{}{"name": n.name}); err != nil {
			log.Fatalf("insert node: %v", err)
		}
	}

	// Insert two edges
	if err := client.InsertEdge(ctx, aliceID, "knows", bobID,   map[string]interface{}{"since": int64(2020)}); err != nil {
		log.Fatalf("insert edge: %v", err)
	}
	if err := client.InsertEdge(ctx, bobID,   "knows", carolID, map[string]interface{}{"since": int64(2022)}); err != nil {
		log.Fatalf("insert edge: %v", err)
	}

	// Pattern query — who does Alice know?
	rows, err := client.Query(ctx, polargraph.QueryRequest{
		Patterns: []polargraph.Pattern{
			{S: aliceID, P: "knows", O: "?friend"},
		},
	})
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	fmt.Println("Alice knows:")
	for _, row := range rows {
		fmt.Printf("  friend=%s\n", row["friend"])
	}

	// Cypher query
	cyRows, err := client.Cypher(ctx, "MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b LIMIT 10")
	if err != nil {
		log.Fatalf("cypher: %v", err)
	}
	fmt.Println("\nCypher MATCH knows:")
	for _, row := range cyRows {
		fmt.Printf("  %v\n", row)
	}

	// Cypher write
	result, err := client.CypherWrite(ctx, `CREATE (c:Company {name: "Acme"})`)
	if err != nil {
		log.Fatalf("cypher write: %v", err)
	}
	fmt.Printf("\nCreated company node IDs: %v\n", result.CreatedNodeIDs)
}

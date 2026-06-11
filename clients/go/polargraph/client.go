// Package polargraph provides an idiomatic Go client for the PolarGraph DB Engine gRPC API.
package polargraph

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	pb "github.com/polarops/polargraph-go/polargraph/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client is a PolarGraph gRPC client.
type Client struct {
	conn   *grpc.ClientConn
	stub   pb.PolarGraphServiceClient
	apiKey string
}

// Option configures a Client at construction time.
type Option func(*clientConfig)

type clientConfig struct {
	apiKey    string
	tlsCACert string
}

// WithAPIKey sets the Bearer token sent on every RPC.
func WithAPIKey(key string) Option {
	return func(c *clientConfig) { c.apiKey = key }
}

// WithTLSCA enables TLS, verifying the server certificate against the PEM file at path.
func WithTLSCA(caCertPath string) Option {
	return func(c *clientConfig) { c.tlsCACert = caCertPath }
}

// New creates a new Client connecting to addr (e.g. "localhost:50051").
func New(addr string, opts ...Option) (*Client, error) {
	cfg := &clientConfig{}
	for _, o := range opts {
		o(cfg)
	}

	var dialOpt grpc.DialOption
	if cfg.tlsCACert != "" {
		pem, err := os.ReadFile(cfg.tlsCACert)
		if err != nil {
			return nil, fmt.Errorf("polargraph: read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("polargraph: invalid CA cert PEM")
		}
		tlsCfg := &tls.Config{RootCAs: pool}
		dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
	} else {
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	conn, err := grpc.NewClient(addr, dialOpt)
	if err != nil {
		return nil, fmt.Errorf("polargraph: dial %s: %w", addr, err)
	}

	return &Client{
		conn:   conn,
		stub:   pb.NewPolarGraphServiceClient(conn),
		apiKey: cfg.apiKey,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) ctx(ctx context.Context) context.Context {
	if c.apiKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.apiKey)
}

// ── Insert ───────────────────────────────────────────────────────────────────

// InsertNode inserts a type label and optional properties for nodeID.
// props values may be bool, int64, float64, string, []byte, or []float32.
func (c *Client) InsertNode(ctx context.Context, nodeID, typeName string, props map[string]interface{}) error {
	triples := []*pb.Triple{
		{Kind: &pb.Triple_Property{Property: &pb.PropertyTriple{
			Subject:   nodeIDProto(nodeID),
			Predicate: "__type",
			Value:     encodeValue(typeName),
		}}},
	}
	for k, v := range props {
		triples = append(triples, &pb.Triple{Kind: &pb.Triple_Property{Property: &pb.PropertyTriple{
			Subject:   nodeIDProto(nodeID),
			Predicate: k,
			Value:     encodeValue(v),
		}}})
	}
	_, err := c.stub.Insert(c.ctx(ctx), &pb.InsertRequest{Triples: triples})
	return err
}

// InsertEdge inserts a directed relation triple between two nodes.
// props become EdgeProperty entries on the relation.
func (c *Client) InsertEdge(ctx context.Context, subject, predicate, object string, props map[string]interface{}) error {
	edgeProps := make([]*pb.EdgeProperty, 0, len(props))
	for k, v := range props {
		edgeProps = append(edgeProps, &pb.EdgeProperty{Name: k, Value: encodeValue(v)})
	}
	triple := &pb.Triple{Kind: &pb.Triple_Relation{Relation: &pb.RelationTriple{
		Subject:    nodeIDProto(subject),
		Predicate:  predicate,
		Object:     nodeIDProto(object),
		Properties: edgeProps,
	}}}
	_, err := c.stub.Insert(c.ctx(ctx), &pb.InsertRequest{Triples: []*pb.Triple{triple}})
	return err
}

// ── Query ────────────────────────────────────────────────────────────────────

// QueryRequest describes a conjunctive pattern query.
type QueryRequest struct {
	Patterns        []Pattern
	Rules           []DatalogRule
	AsOfValidTime   int64
	AsOfTxTime      int64
}

// Pattern is one triple pattern in a query.
// Subject/Object: "" = wildcard, "?varname" = variable, uuid string = bound.
type Pattern struct {
	S, P, O string
}

// DatalogRule is a Datalog rule for recursive queries.
type DatalogRule struct {
	HeadPredicate  string
	HeadSubjectVar string
	HeadObjectVar  string
	Body           []Pattern
}

// Query executes a conjunctive pattern query and returns binding maps.
// Each map entry maps variable name (without '?') to node-ID string.
func (c *Client) Query(ctx context.Context, req QueryRequest) ([]map[string]string, error) {
	pbPatterns := make([]*pb.VarPattern, len(req.Patterns))
	for i, p := range req.Patterns {
		pbPatterns[i] = patternProto(p)
	}
	pbRules := make([]*pb.DatalogRule, len(req.Rules))
	for i, r := range req.Rules {
		pbRules[i] = ruleProto(r)
	}
	resp, err := c.stub.Query(c.ctx(ctx), &pb.QueryRequest{
		Patterns:      pbPatterns,
		Rules:         pbRules,
		AsOfValidTime: req.AsOfValidTime,
		AsOfTxTime:    req.AsOfTxTime,
	})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]string, len(resp.Bindings))
	for i, b := range resp.Bindings {
		row := make(map[string]string, len(b.Vars))
		for k, v := range b.Vars {
			row[k] = uuidString(v)
		}
		out[i] = row
	}
	return out, nil
}

// ── Cypher ───────────────────────────────────────────────────────────────────

// CypherOption configures a Cypher query call.
type CypherOption func(*pb.CypherQueryRequest)

// WithVector provides the embedding for a VECTOR_NEAR Cypher clause.
func WithVector(vec []float32) CypherOption {
	return func(r *pb.CypherQueryRequest) { r.Vector = vec }
}

// WithEF sets the HNSW exploration factor for VECTOR_NEAR.
func WithEF(ef uint32) CypherOption {
	return func(r *pb.CypherQueryRequest) { r.Ef = ef }
}

// Cypher executes a Cypher read query. Each result row is a map of variable
// name to either a node-ID string (for graph nodes) or a scalar Go value.
func (c *Client) Cypher(ctx context.Context, query string, opts ...CypherOption) ([]map[string]interface{}, error) {
	req := &pb.CypherQueryRequest{Cypher: query}
	for _, o := range opts {
		o(req)
	}
	resp, err := c.stub.CypherQuery(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, len(resp.Rows))
	for i, row := range resp.Rows {
		m := make(map[string]interface{}, len(row.Nodes)+len(row.Values))
		for k, v := range row.Nodes {
			m[k] = uuidString(v)
		}
		for k, v := range row.Values {
			m[k] = decodeValue(v)
		}
		out[i] = m
	}
	return out, nil
}

// WriteResult is returned by CypherWrite.
type WriteResult struct {
	CreatedNodeIDs []string
	TriplesWritten uint64
	TriplesDeleted uint64
}

// CypherWrite executes a Cypher write statement (CREATE/MERGE/SET/DELETE).
func (c *Client) CypherWrite(ctx context.Context, query string) (*WriteResult, error) {
	resp, err := c.stub.CypherWrite(c.ctx(ctx), &pb.CypherWriteRequest{Cypher: query})
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(resp.CreatedNodeIds))
	for i, b := range resp.CreatedNodeIds {
		ids[i] = uuidFromBytes(b)
	}
	return &WriteResult{
		CreatedNodeIDs: ids,
		TriplesWritten: resp.TriplesWritten,
		TriplesDeleted: resp.TriplesDeleted,
	}, nil
}

// ── Vector ───────────────────────────────────────────────────────────────────

// SearchOption configures a vector search call.
type SearchOption func(*pb.SearchVectorRequest)

// WithSearchEF sets the HNSW exploration factor.
func WithSearchEF(ef uint32) SearchOption {
	return func(r *pb.SearchVectorRequest) { r.Ef = ef }
}

// SearchResult is one nearest-neighbour result.
type SearchResult struct {
	NodeID     string
	Similarity float32
}

// SearchVector searches for the k nearest neighbours in space.
func (c *Client) SearchVector(ctx context.Context, space string, vec []float32, k int, opts ...SearchOption) ([]SearchResult, error) {
	req := &pb.SearchVectorRequest{Query: vec, K: uint32(k), Space: space}
	for _, o := range opts {
		o(req)
	}
	resp, err := c.stub.SearchVector(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, len(resp.Results))
	for i, r := range resp.Results {
		out[i] = SearchResult{NodeID: uuidString(r.NodeId), Similarity: r.Similarity}
	}
	return out, nil
}

// InsertVector inserts or updates an embedding for nodeID in space.
func (c *Client) InsertVector(ctx context.Context, nodeID string, vec []float32, space string) error {
	_, err := c.stub.InsertVector(c.ctx(ctx), &pb.InsertVectorRequest{
		NodeId: nodeIDProto(nodeID),
		Vector: vec,
		Space:  space,
	})
	return err
}

// ── Transactions (stubbed — see roadmap §4) ───────────────────────────────────

// BeginTx is reserved for the wire-transaction API (roadmap §4).
func (c *Client) BeginTx(ctx context.Context) (string, error) {
	return "", fmt.Errorf("polargraph: wire transactions not yet implemented in polargraphd (see roadmap §4)")
}

// CommitTx is reserved for the wire-transaction API (roadmap §4).
func (c *Client) CommitTx(ctx context.Context, txID string) error {
	return fmt.Errorf("polargraph: wire transactions not yet implemented")
}

// RollbackTx is reserved for the wire-transaction API (roadmap §4).
func (c *Client) RollbackTx(ctx context.Context, txID string) error {
	return fmt.Errorf("polargraph: wire transactions not yet implemented")
}

// ── Reachability ─────────────────────────────────────────────────────────────

// Reachable returns all node IDs reachable from start via predicate.
// maxHops=0 means unlimited depth.
func (c *Client) Reachable(ctx context.Context, start, predicate string, maxHops uint32) ([]string, error) {
	resp, err := c.stub.Reachable(c.ctx(ctx), &pb.ReachableRequest{
		Start:     nodeIDProto(start),
		Predicate: predicate,
		MaxHops:   maxHops,
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, len(resp.NodeIds))
	for i, n := range resp.NodeIds {
		out[i] = uuidString(n)
	}
	return out, nil
}

package polargraph

import (
	"context"
	"fmt"

	pb "github.com/polarops/polargraph-go/polargraph/proto"
)

// RDFType is the rdf:type predicate IRI. Node types are rdf:type relations
// to class IRIs.
const RDFType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

// WriteMode is how a property add treats existing values of the same
// (subject, predicate, graph).
type WriteMode int32

const (
	// WriteAuto replaces an open-ended value (the default).
	WriteAuto WriteMode = WriteMode(pb.PropertyWriteMode_PROPERTY_WRITE_MODE_AUTO)
	// WriteReplace closes every other open value first.
	WriteReplace WriteMode = WriteMode(pb.PropertyWriteMode_PROPERTY_WRITE_MODE_REPLACE)
	// WriteAdd keeps existing values (multi-valued properties).
	WriteAdd WriteMode = WriteMode(pb.PropertyWriteMode_PROPERTY_WRITE_MODE_ADD)
)

// Change is one add in a ChangeSet: a property when Value is non-nil,
// otherwise a relation to Object (a node UUID) or ObjectIRI (a full IRI,
// prefix:local or bare vocabulary name, resolved by the server).
type Change struct {
	Subject   string
	Predicate string
	Object    string
	ObjectIRI string
	Value     interface{}
	Mode      WriteMode
}

// Retraction names an exact live quad to close: a relation to Object, or a
// property with Value. Graph "" is the default graph.
type Retraction struct {
	Subject   string
	Predicate string
	Object    string
	Value     interface{}
	Graph     string
}

// ChangeSet is applied atomically by ApplyChanges.
type ChangeSet struct {
	// Adds by graph IRI ("" = default graph).
	Adds        map[string][]Change
	Retractions []Retraction
	// ReadTS, when non-zero, aborts the changeset if a touched quad was
	// committed after it.
	ReadTS int64
	// Strict fails the changeset if a retraction matches no live quad.
	Strict bool
	// IRIs to record in the IRI dictionary.
	IRIs []string
}

// ChangeResult is returned by ApplyChanges.
type ChangeResult struct {
	CommitTS            int64
	Added               uint64
	Retracted           uint64
	RetractionsNotFound uint64
	EdgeIDs             []string
}

func changeProto(ch Change) (*pb.Triple, error) {
	if ch.Value != nil {
		return &pb.Triple{Kind: &pb.Triple_Property{Property: &pb.PropertyTriple{
			Subject:   nodeIDProto(ch.Subject),
			Predicate: ch.Predicate,
			Value:     encodeValue(ch.Value),
			Mode:      pb.PropertyWriteMode(ch.Mode),
		}}}, nil
	}
	if ch.Object == "" && ch.ObjectIRI == "" {
		return nil, fmt.Errorf("change %s %s: set Value, Object or ObjectIRI", ch.Subject, ch.Predicate)
	}
	rel := &pb.RelationTriple{
		Subject:   nodeIDProto(ch.Subject),
		Predicate: ch.Predicate,
		ObjectIri: ch.ObjectIRI,
	}
	if ch.Object != "" {
		rel.Object = nodeIDProto(ch.Object)
	}
	return &pb.Triple{Kind: &pb.Triple_Relation{Relation: rel}}, nil
}

func retractionProto(r Retraction) *pb.QuadRef {
	ref := &pb.QuadRef{
		Subject:   nodeIDProto(r.Subject),
		Predicate: r.Predicate,
		Graph:     r.Graph,
	}
	if r.Value != nil {
		ref.Object = &pb.QuadRef_Value{Value: encodeValue(r.Value)}
	} else {
		ref.Object = &pb.QuadRef_Node{Node: nodeIDProto(r.Object)}
	}
	return ref
}

// ApplyChanges applies adds and retractions across graphs in one
// transaction (one commit, one change-feed entry). It replaces the
// deprecated CypherWrite.
func (c *Client) ApplyChanges(ctx context.Context, cs ChangeSet) (*ChangeResult, error) {
	req := &pb.ApplyChangesRequest{ReadTs: cs.ReadTS, Strict: cs.Strict, Iris: cs.IRIs}
	for graph, changes := range cs.Adds {
		group := &pb.GraphTriples{Graph: graph}
		for _, ch := range changes {
			t, err := changeProto(ch)
			if err != nil {
				return nil, err
			}
			group.Triples = append(group.Triples, t)
		}
		req.Adds = append(req.Adds, group)
	}
	for _, r := range cs.Retractions {
		req.Retractions = append(req.Retractions, retractionProto(r))
	}
	resp, err := c.stub.ApplyChanges(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(resp.EdgeIds))
	for i, b := range resp.EdgeIds {
		ids[i] = uuidFromBytes(b)
	}
	return &ChangeResult{
		CommitTS:            resp.CommitTs,
		Added:               resp.Added,
		Retracted:           resp.Retracted,
		RetractionsNotFound: resp.RetractionsNotFound,
		EdgeIDs:             ids,
	}, nil
}

// ── Vocabulary ───────────────────────────────────────────────────────────────

// LegacyStatus reports pre-vocabulary data awaiting ConvertLegacyData.
type LegacyStatus struct {
	ConversionPending bool
	BarePredicates    []string
	TypeLabels        uint64
	PendingMerges     []string
}

// Vocabulary is the server's base IRI for bare names and its prefixes.
type Vocabulary struct {
	Base     string
	Prefixes map[string]string
	Legacy   LegacyStatus
}

// PredicateConversion is one bare predicate converted to an IRI.
type PredicateConversion struct {
	From, To   string
	Merged     bool
	QuadsMoved uint64
}

// ConversionReport is returned by ConvertLegacyData.
type ConversionReport struct {
	DryRun          bool
	Predicates      []PredicateConversion
	LabelsConverted uint64
	Legacy          LegacyStatus
}

func legacyFromProto(l *pb.LegacyStatus) LegacyStatus {
	if l == nil {
		return LegacyStatus{}
	}
	return LegacyStatus{
		ConversionPending: l.ConversionPending,
		BarePredicates:    l.BarePredicates,
		TypeLabels:        l.TypeLabels,
		PendingMerges:     l.PendingMerges,
	}
}

func vocabularyFromProto(v *pb.Vocabulary) *Vocabulary {
	out := &Vocabulary{Base: v.Base, Prefixes: map[string]string{}, Legacy: legacyFromProto(v.Legacy)}
	for _, p := range v.Prefixes {
		out.Prefixes[p.Name] = p.Namespace
	}
	return out
}

// GetVocabulary returns the base IRI, prefixes and legacy-conversion status.
func (c *Client) GetVocabulary(ctx context.Context) (*Vocabulary, error) {
	v, err := c.stub.GetVocabulary(c.ctx(ctx), &pb.GetVocabularyRequest{})
	if err != nil {
		return nil, err
	}
	return vocabularyFromProto(v), nil
}

// SetVocabularyBase sets the base IRI for bare names (service call, primary).
func (c *Client) SetVocabularyBase(ctx context.Context, base string) (*Vocabulary, error) {
	v, err := c.stub.SetVocabularyBase(c.ctx(ctx), &pb.SetVocabularyBaseRequest{Base: base})
	if err != nil {
		return nil, err
	}
	return vocabularyFromProto(v), nil
}

// PutPrefix declares or re-points a prefix.
func (c *Client) PutPrefix(ctx context.Context, name, namespace string) (*Vocabulary, error) {
	v, err := c.stub.PutPrefix(c.ctx(ctx), &pb.PutPrefixRequest{Name: name, Namespace: namespace})
	if err != nil {
		return nil, err
	}
	return vocabularyFromProto(v), nil
}

// RemovePrefix removes a prefix (no-op if absent).
func (c *Client) RemovePrefix(ctx context.Context, name string) (*Vocabulary, error) {
	v, err := c.stub.RemovePrefix(c.ctx(ctx), &pb.RemovePrefixRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return vocabularyFromProto(v), nil
}

// ConvertLegacyData runs (or, with dryRun, reports) the one-time conversion
// of pre-vocabulary data. Idempotent and resumable.
func (c *Client) ConvertLegacyData(ctx context.Context, dryRun bool) (*ConversionReport, error) {
	r, err := c.stub.ConvertLegacyData(c.ctx(ctx), &pb.ConvertLegacyDataRequest{DryRun: dryRun})
	if err != nil {
		return nil, err
	}
	out := &ConversionReport{DryRun: r.DryRun, LabelsConverted: r.LabelsConverted, Legacy: legacyFromProto(r.Legacy)}
	for _, p := range r.Predicates {
		out.Predicates = append(out.Predicates, PredicateConversion{
			From: p.From, To: p.To, Merged: p.Merged, QuadsMoved: p.QuadsMoved,
		})
	}
	return out, nil
}

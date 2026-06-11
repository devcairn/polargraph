package polargraph

import (
	"encoding/hex"
	"fmt"
	"strings"

	pb "github.com/polarops/polargraph-go/polargraph/proto"
)

// uuidString converts a proto NodeId to a hyphenated UUID string.
func uuidString(n *pb.NodeId) string {
	if n == nil || len(n.Bytes) != 16 {
		return ""
	}
	return uuidFromBytes(n.Bytes)
}

func uuidFromBytes(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	h := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

// nodeIDProto parses a hyphenated UUID string to a proto NodeId.
func nodeIDProto(s string) *pb.NodeId {
	clean := strings.ReplaceAll(s, "-", "")
	b, err := hex.DecodeString(clean)
	if err != nil || len(b) != 16 {
		return &pb.NodeId{Bytes: make([]byte, 16)}
	}
	return &pb.NodeId{Bytes: b}
}

// encodeValue converts a Go value to a proto Value.
// Supported types: nil, bool, int/int64, float32/float64, string, []byte, []float32.
func encodeValue(v interface{}) *pb.Value {
	if v == nil {
		return &pb.Value{Kind: &pb.Value_NullVal{NullVal: true}}
	}
	switch x := v.(type) {
	case bool:
		return &pb.Value{Kind: &pb.Value_BoolVal{BoolVal: x}}
	case int:
		return &pb.Value{Kind: &pb.Value_IntVal{IntVal: int64(x)}}
	case int32:
		return &pb.Value{Kind: &pb.Value_IntVal{IntVal: int64(x)}}
	case int64:
		return &pb.Value{Kind: &pb.Value_IntVal{IntVal: x}}
	case float32:
		return &pb.Value{Kind: &pb.Value_FloatVal{FloatVal: float64(x)}}
	case float64:
		return &pb.Value{Kind: &pb.Value_FloatVal{FloatVal: x}}
	case string:
		return &pb.Value{Kind: &pb.Value_TextVal{TextVal: x}}
	case []byte:
		return &pb.Value{Kind: &pb.Value_BlobVal{BlobVal: x}}
	case []float32:
		return &pb.Value{Kind: &pb.Value_VecVal{VecVal: &pb.FloatArray{Values: x}}}
	default:
		return &pb.Value{Kind: &pb.Value_TextVal{TextVal: fmt.Sprintf("%v", v)}}
	}
}

// decodeValue converts a proto Value to a Go value.
func decodeValue(v *pb.Value) interface{} {
	if v == nil {
		return nil
	}
	switch x := v.Kind.(type) {
	case *pb.Value_NullVal:
		return nil
	case *pb.Value_BoolVal:
		return x.BoolVal
	case *pb.Value_IntVal:
		return x.IntVal
	case *pb.Value_FloatVal:
		return x.FloatVal
	case *pb.Value_TextVal:
		return x.TextVal
	case *pb.Value_BlobVal:
		return x.BlobVal
	case *pb.Value_VecVal:
		if x.VecVal == nil {
			return []float32{}
		}
		return x.VecVal.Values
	default:
		return nil
	}
}

// patternProto converts a Pattern to a proto VarPattern.
func patternProto(p Pattern) *pb.VarPattern {
	vp := &pb.VarPattern{Predicate: strings.TrimPrefix(p.P, ":")}
	if t := makeTerm(p.S); t != nil {
		vp.Subject = t
	}
	if t := makeTerm(p.O); t != nil {
		vp.Object = t
	}
	return vp
}

// makeTerm converts a pattern slot string to a proto Term.
// "" or "_" → nil (wildcard), "?varname" → var term, uuid → bound term.
func makeTerm(s string) *pb.Term {
	if s == "" || s == "_" {
		return nil
	}
	if strings.HasPrefix(s, "?") {
		return &pb.Term{Kind: &pb.Term_Var{Var: strings.TrimPrefix(s, "?")}}
	}
	return &pb.Term{Kind: &pb.Term_Bound{Bound: nodeIDProto(s)}}
}

// ruleProto converts a DatalogRule to the proto representation.
func ruleProto(r DatalogRule) *pb.DatalogRule {
	body := make([]*pb.VarPattern, len(r.Body))
	for i, p := range r.Body {
		body[i] = patternProto(p)
	}
	return &pb.DatalogRule{
		HeadPredicate:   r.HeadPredicate,
		HeadSubjectVar:  r.HeadSubjectVar,
		HeadObjectVar:   r.HeadObjectVar,
		Body:            body,
	}
}

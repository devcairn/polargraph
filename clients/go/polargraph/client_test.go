package polargraph

import (
	"testing"

	pb "github.com/polarops/polargraph-go/polargraph/proto"
)

func TestUUIDRoundTrip(t *testing.T) {
	original := "01234567-89ab-cdef-0123-456789abcdef"
	proto := nodeIDProto(original)
	if len(proto.Bytes) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(proto.Bytes))
	}
	back := uuidString(proto)
	if back != original {
		t.Fatalf("round-trip mismatch: got %q, want %q", back, original)
	}
}

func TestEncodeDecodeValue(t *testing.T) {
	cases := []struct {
		in   interface{}
		want interface{}
	}{
		{nil, nil},
		{true, true},
		{int64(42), int64(42)},
		{float64(3.14), 3.14},
		{"hello", "hello"},
		{[]byte("raw"), []byte("raw")},
	}
	for _, tc := range cases {
		enc := encodeValue(tc.in)
		got := decodeValue(enc)
		switch want := tc.want.(type) {
		case []byte:
			gotBytes, ok := got.([]byte)
			if !ok {
				t.Errorf("encodeValue(%v): got type %T, want []byte", tc.in, got)
				continue
			}
			if string(gotBytes) != string(want) {
				t.Errorf("encodeValue(%v): got %v, want %v", tc.in, gotBytes, want)
			}
		default:
			if got != want {
				t.Errorf("encodeValue(%v): got %v (%T), want %v (%T)", tc.in, got, got, want, want)
			}
		}
	}
}

func TestPatternProto_Variable(t *testing.T) {
	p := Pattern{S: "?a", P: ":knows", O: "?b"}
	vp := patternProto(p)
	if vp.Predicate != "knows" {
		t.Errorf("predicate: got %q, want %q", vp.Predicate, "knows")
	}
	sVar, ok := vp.Subject.Kind.(*pb.Term_Var)
	if !ok {
		t.Fatalf("subject not a var term: %T", vp.Subject.Kind)
	}
	if sVar.Var != "a" {
		t.Errorf("subject var: got %q, want %q", sVar.Var, "a")
	}
	oVar, ok := vp.Object.Kind.(*pb.Term_Var)
	if !ok {
		t.Fatalf("object not a var term: %T", vp.Object.Kind)
	}
	if oVar.Var != "b" {
		t.Errorf("object var: got %q, want %q", oVar.Var, "b")
	}
}

func TestPatternProto_Wildcard(t *testing.T) {
	p := Pattern{S: "?n", P: "", O: ""}
	vp := patternProto(p)
	if vp.Subject == nil {
		t.Fatal("subject should not be nil for variable")
	}
	if vp.Object != nil {
		t.Errorf("object wildcard should be nil term, got %v", vp.Object)
	}
}

func TestMakeTermNil(t *testing.T) {
	if makeTerm("") != nil {
		t.Error("empty string should produce nil term")
	}
	if makeTerm("_") != nil {
		t.Error("underscore should produce nil term (wildcard)")
	}
}

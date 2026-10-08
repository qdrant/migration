package commons

import (
	"testing"

	"github.com/qdrant/go-client/qdrant"
)

func TestPointIDRoundTrip(t *testing.T) {
	for _, id := range []*qdrant.PointId{
		qdrant.NewIDNum(0),
		qdrant.NewIDNum(18446744073709551615),
		qdrant.NewIDUUID("0f4a6de3-c18b-5de3-992b-5eb7f5c52b1a"),
	} {
		s, err := EncodePointID(id)
		if err != nil {
			t.Fatalf("encode %v: %v", id, err)
		}
		got, err := DecodePointID(s)
		if err != nil {
			t.Fatalf("decode %q: %v", s, err)
		}
		if got.GetNum() != id.GetNum() || got.GetUuid() != id.GetUuid() {
			t.Fatalf("round trip mismatch: %v != %v", got, id)
		}
	}
	if _, err := DecodePointID("x:1"); err == nil {
		t.Fatal("expected error for invalid encoding")
	}
}

func TestBoundariesFingerprint(t *testing.T) {
	a := []*qdrant.PointId{qdrant.NewIDNum(1), qdrant.NewIDNum(5)}
	b := []*qdrant.PointId{qdrant.NewIDNum(1), qdrant.NewIDNum(5)}
	c := []*qdrant.PointId{qdrant.NewIDNum(1), qdrant.NewIDNum(6)}

	fa, _ := BoundariesFingerprint(a)
	fb, _ := BoundariesFingerprint(b)
	fc, _ := BoundariesFingerprint(c)
	if fa != fb {
		t.Fatalf("same boundaries must give the same fingerprint: %s != %s", fa, fb)
	}
	if fa == fc {
		t.Fatalf("different boundaries must give different fingerprints: %s == %s", fa, fc)
	}
}

func TestBoundariesFingerprintIsStable(t *testing.T) {
	ids := []*qdrant.PointId{qdrant.NewIDNum(1), qdrant.NewIDUUID("0f4a6de3-c18b-5de3-992b-5eb7f5c52b1a")}
	got, err := BoundariesFingerprint(ids)
	if err != nil {
		t.Fatal(err)
	}
	if got != "ecdb964d138d" {
		t.Fatalf("fingerprint changed: got %s", got)
	}
}

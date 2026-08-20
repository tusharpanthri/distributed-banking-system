package shared

import "testing"

func TestBallotOrdering(t *testing.T) {
	tests := []struct {
		name string
		a, b Ballot
		want int
	}{
		{"lower round sorts first", Ballot{1, 9}, Ballot{2, 1}, -1},
		{"higher round sorts last", Ballot{3, 1}, Ballot{2, 9}, 1},
		{"same round breaks on server id", Ballot{2, 1}, Ballot{2, 3}, -1},
		{"identical ballots are equal", Ballot{2, 3}, Ballot{2, 3}, 0},
		{"zero ballot precedes everything", Ballot{}, Ballot{1, 1}, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Fatalf("Compare(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
			if got := tc.b.Compare(tc.a); got != -tc.want {
				t.Fatalf("Compare is not antisymmetric for %v / %v", tc.a, tc.b)
			}
		})
	}
}

// The whole point of carrying ServerID: two servers reaching the same round
// counter must still produce distinguishable, strictly ordered ballots.
func TestSameRoundDifferentServersAreOrdered(t *testing.T) {
	s2 := Ballot{Number: 5, ServerID: 2}
	s3 := Ballot{Number: 5, ServerID: 3}

	if s2.Compare(s3) == 0 {
		t.Fatal("two servers at the same round produced indistinguishable ballots")
	}
	if !s3.GreaterThan(s2) {
		t.Fatalf("%v should follow %v", s3, s2)
	}
	if s2.GreaterThan(s3) {
		t.Fatalf("%v should not follow %v", s2, s3)
	}
}

func TestAtLeast(t *testing.T) {
	b := Ballot{Number: 4, ServerID: 2}
	if !b.AtLeast(b) {
		t.Fatal("a ballot should honour a promise made at itself")
	}
	if b.AtLeast(Ballot{Number: 4, ServerID: 3}) {
		t.Fatal("a ballot must not honour a promise made to a higher ballot")
	}
	if !b.AtLeast(Ballot{Number: 4, ServerID: 1}) {
		t.Fatal("a ballot should honour a promise made to a lower ballot")
	}
}

func TestNextAlwaysAdvances(t *testing.T) {
	seen := Ballot{Number: 7, ServerID: 9}
	next := seen.Next(1)

	if !next.GreaterThan(seen) {
		t.Fatalf("Next(%v) = %v, which does not advance", seen, next)
	}
	if next.ServerID != 1 {
		t.Fatalf("Next did not stamp the proposing server: %v", next)
	}
}

func TestServerNumber(t *testing.T) {
	if n, err := ServerNumber("S12"); err != nil || n != 12 {
		t.Fatalf("ServerNumber(S12) = %d, %v", n, err)
	}
	for _, bad := range []string{"", "S", "12", "X3", "Sx"} {
		if _, err := ServerNumber(bad); err == nil {
			t.Fatalf("ServerNumber(%q) should have failed", bad)
		}
	}
}

func TestQuorumSizeIsStrictMajority(t *testing.T) {
	tests := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4}
	for n, want := range tests {
		SetServersPerCluster(n)
		if got := GetQuorumSize(); got != want {
			t.Errorf("GetQuorumSize() with %d servers = %d, want %d", n, got, want)
		}
		// Two disjoint quorums must be impossible.
		if got := GetQuorumSize(); 2*got <= n {
			t.Errorf("quorum of %d out of %d servers allows two disjoint quorums", got, n)
		}
	}
}

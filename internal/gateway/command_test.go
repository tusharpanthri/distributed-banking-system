package gateway

import (
	"reflect"
	"testing"
)

func TestParseAcceptsValidCommands(t *testing.T) {
	tests := []struct {
		line string
		want command
	}{
		{"status", command{Verb: verbStatus}},
		{"  status  ", command{Verb: verbStatus}},
		{"STATUS", command{Verb: verbStatus}},
		{"heal", command{Verb: verbHeal}},
		{"help", command{Verb: verbHelp}},
		{"clear", command{Verb: verbClear}},
		{"put alice 100", command{Verb: verbPut, Key: "alice", Value: 100}},
		{"put alice -5", command{Verb: verbPut, Key: "alice", Value: -5}},
		{"get alice", command{Verb: verbGet, Key: "alice"}},
		{"transfer alice bob 25", command{Verb: verbTransfer, From: "alice", To: "bob", Value: 25}},
		{"kill s0n1", command{Verb: verbKill, Node: "s0n1"}},
		{"kill S0N1", command{Verb: verbKill, Node: "s0n1"}},
		{"revive s2n0", command{Verb: verbRevive, Node: "s2n0"}},
	}

	for _, tt := range tests {
		got, err := parse(tt.line)
		if err != nil {
			t.Errorf("parse(%q) returned error %v", tt.line, err)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("parse(%q) = %+v, want %+v", tt.line, got, tt.want)
		}
	}
}

// The pipe is natural to type either spaced or butted against the ids, and a
// user who has to guess which one works will guess wrong half the time.
func TestParsePartitionAcceptsBothPipeSpacings(t *testing.T) {
	want := [][]string{{"s0n0"}, {"s0n1", "s0n2"}}

	for _, line := range []string{
		"partition s0n0 | s0n1 s0n2",
		"partition s0n0|s0n1 s0n2",
		"partition  s0n0   |   s0n1   s0n2  ",
	} {
		got, err := parse(line)
		if err != nil {
			t.Errorf("parse(%q) returned error %v", line, err)
			continue
		}
		if !reflect.DeepEqual(got.Groups, want) {
			t.Errorf("parse(%q) groups = %v, want %v", line, got.Groups, want)
		}
	}
}

func TestParsePartitionSupportsThreeWaySplit(t *testing.T) {
	got, err := parse("partition s0n0 | s0n1 | s0n2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := [][]string{{"s0n0"}, {"s0n1"}, {"s0n2"}}
	if !reflect.DeepEqual(got.Groups, want) {
		t.Errorf("groups = %v, want %v", got.Groups, want)
	}
}

func TestParseRejectsMalformedCommands(t *testing.T) {
	for _, line := range []string{
		"frobnicate",
		"status now",
		"put",
		"put alice",
		"put alice bob", // value is not an integer
		"put alice 1 2", // too many arguments
		"put ali ce 1",  // key with a space cannot round-trip
		"put al!ce 1",   // punctuation is not allowed in keys
		"get",
		"get a b",
		"transfer alice bob",
		"transfer alice bob xyz",
		"transfer alice bob 0", // amount must be positive
		"transfer alice bob -5",
		"transfer alice alice 5", // same account both sides
		"kill",
		"kill a b",
		"revive",
		"partition",             // no groups
		"partition s0n0",        // only one group
		"partition s0n0 |",      // empty trailing group
		"partition | s0n0",      // empty leading group
		"partition s0n0 | s0n0", // same node in two groups
		"heal now",
	} {
		if _, err := parse(line); err == nil {
			t.Errorf("parse(%q) should have failed", line)
		}
	}
}

// A blank line is not an error — the session swallows it rather than answering
// with a complaint every time someone hits enter.
func TestParseTreatsBlankLinesAsEmpty(t *testing.T) {
	for _, line := range []string{"", "   ", "\t"} {
		if _, err := parse(line); err != errEmpty {
			t.Errorf("parse(%q) error = %v, want errEmpty", line, err)
		}
	}
}

func TestParseRejectsOverlongKey(t *testing.T) {
	long := ""
	for len(long) <= maxKeyLen {
		long += "a"
	}
	if _, err := parse("get " + long); err == nil {
		t.Errorf("parse should reject a key longer than %d characters", maxKeyLen)
	}
}

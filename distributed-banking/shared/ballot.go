package shared

import (
	"fmt"
	"strconv"
)

// Ballot is a Paxos ballot number. A bare counter is not enough: two leaders in
// the same cluster can independently reach the same counter value, and an
// acceptor comparing only counters has no way to tell those two proposals
// apart. Pairing the counter with the proposer's server number gives every
// ballot a unique, totally ordered identity, which is what the Paxos safety
// argument actually requires.
//
// Ordering is lexicographic: Number first, ServerID as the tiebreak.
type Ballot struct {
	Number   int // Monotonically increasing round counter
	ServerID int // Numeric ID of the proposing server, breaks ties
}

// NewBallot builds a ballot for a server ID in the "S<n>" form used throughout
// the system.
func NewBallot(number int, serverID string) (Ballot, error) {
	n, err := ServerNumber(serverID)
	if err != nil {
		return Ballot{}, err
	}
	return Ballot{Number: number, ServerID: n}, nil
}

// ServerNumber extracts the numeric suffix from a server ID such as "S3".
func ServerNumber(serverID string) (int, error) {
	if len(serverID) < 2 || serverID[0] != 'S' {
		return 0, fmt.Errorf("invalid server ID format: %s", serverID)
	}
	n, err := strconv.Atoi(serverID[1:])
	if err != nil {
		return 0, fmt.Errorf("invalid server ID format: %s", serverID)
	}
	return n, nil
}

// Compare returns -1 if b sorts before other, 0 if they are the same ballot,
// and +1 if b sorts after other.
func (b Ballot) Compare(other Ballot) int {
	switch {
	case b.Number != other.Number:
		if b.Number < other.Number {
			return -1
		}
		return 1
	case b.ServerID != other.ServerID:
		if b.ServerID < other.ServerID {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// GreaterThan reports whether b strictly follows other in the total order.
func (b Ballot) GreaterThan(other Ballot) bool { return b.Compare(other) > 0 }

// AtLeast reports whether b is other or follows it. Acceptors use this to
// decide whether a proposal still honours an outstanding promise.
func (b Ballot) AtLeast(other Ballot) bool { return b.Compare(other) >= 0 }

// IsZero reports whether b is the zero ballot, meaning no promise has been made
// and no value accepted.
func (b Ballot) IsZero() bool { return b.Number == 0 && b.ServerID == 0 }

// Next returns the next ballot this server can propose, one round above the
// highest ballot it has seen.
func (b Ballot) Next(serverID int) Ballot {
	return Ballot{Number: b.Number + 1, ServerID: serverID}
}

// String renders the ballot in the <number,server> form the datastore output
// already uses.
func (b Ballot) String() string { return fmt.Sprintf("<%d,%d>", b.Number, b.ServerID) }

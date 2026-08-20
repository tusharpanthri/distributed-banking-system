package shared

type PrepareRequest struct {
	CommittedTransactions []Transaction
	LeaderBallot          Ballot
}

// PromiseReply is an acceptor's answer to a prepare. A promise is only
// meaningful if it reports what the acceptor has already accepted, so the
// leader can adopt the highest-ballot value already in flight instead of
// overwriting it.
type PromiseReply struct {
	ServerID       string
	Promised       bool          // False when the acceptor has promised a higher ballot
	PromisedBallot Ballot        // The ballot the acceptor is currently promised to
	AcceptedBallot Ballot        // Highest ballot the acceptor has accepted a value under
	AcceptedLog    []Transaction // Log the acceptor holds at AcceptedBallot
}

// AcceptReply is an acceptor's answer to an accept.
type AcceptReply struct {
	ServerID       string
	Accepted       bool
	PromisedBallot Ballot // Set when the accept was rejected, so the leader can catch up
}

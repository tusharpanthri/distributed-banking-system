package paxos

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"distributed-banking/shared"
)

const (
	// A dead peer must fail fast rather than hanging the leader. net/rpc has no
	// built-in deadline, so every call is raced against a timer.
	dialTimeout = 500 * time.Millisecond
	callTimeout = 1 * time.Second
)

// dial opens a connection to a peer with a bounded handshake.
func dial(serverID string) (*rpc.Client, error) {
	addr, err := shared.ServerAddresses(serverID)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

// callWithTimeout issues an RPC and gives up after callTimeout. Without this a
// single unresponsive peer blocks the entire phase.
func callWithTimeout(client *rpc.Client, method string, args, reply interface{}) error {
	done := make(chan *rpc.Call, 1)
	client.Go(method, args, reply, done)

	select {
	case call := <-done:
		return call.Error
	case <-time.After(callTimeout):
		// The reply is abandoned: the in-flight call may still write to it, so
		// callers must not read it once this returns an error.
		return fmt.Errorf("rpc %s timed out after %s", method, callTimeout)
	}
}

// PrepareResult is what the leader learned from a prepare round.
type PrepareResult struct {
	Promises int           // Servers that promised, including the leader itself
	Quorum   bool          // Whether Promises reached a strict majority
	MaxSeen  shared.Ballot // Highest ballot any acceptor reported, for catch-up
	// AdoptedLog is the log held by the acceptor with the highest accepted
	// ballot. Paxos requires the leader to re-propose an in-flight value rather
	// than replace it, so this may not be the leader's own log.
	AdoptedLog      []shared.Transaction
	AdoptedFrom     shared.Ballot
	adoptedFromPeer bool
}

// PreparePhase sends prepares to every peer concurrently and reports how many
// promised. The leader counts as one promise: it is an acceptor too, and it has
// trivially promised to its own ballot.
func PreparePhase(leaderID string, activeServers []string, leaderLog []shared.Transaction, ballot shared.Ballot) PrepareResult {
	replies := broadcast(leaderID, activeServers, func(serverID string, client *rpc.Client) (shared.PromiseReply, bool) {
		var promise shared.PromiseReply
		req := shared.PrepareRequest{CommittedTransactions: leaderLog, LeaderBallot: ballot}
		if err := callWithTimeout(client, fmt.Sprintf("Server.%s.Prepare", serverID), req, &promise); err != nil {
			return shared.PromiseReply{}, false
		}
		return promise, true
	})

	result := PrepareResult{
		Promises:    1, // the leader promises to itself
		MaxSeen:     ballot,
		AdoptedLog:  leaderLog,
		AdoptedFrom: shared.Ballot{},
	}

	for _, promise := range replies {
		if promise.PromisedBallot.GreaterThan(result.MaxSeen) {
			result.MaxSeen = promise.PromisedBallot
		}
		if !promise.Promised {
			// A rejection still tells us someone is further ahead.
			continue
		}
		result.Promises++
		// Adopt the value accepted under the highest ballot seen in the quorum.
		if !promise.AcceptedBallot.IsZero() && promise.AcceptedBallot.GreaterThan(result.AdoptedFrom) {
			result.AdoptedFrom = promise.AcceptedBallot
			result.AdoptedLog = promise.AcceptedLog
			result.adoptedFromPeer = true
		}
	}

	result.Quorum = result.Promises >= shared.GetQuorumSize()
	return result
}

// AcceptResult is what the leader learned from an accept round.
type AcceptResult struct {
	Accepts int
	Quorum  bool
	MaxSeen shared.Ballot
}

// AcceptPhase sends accepts concurrently and counts the acks. The leader only
// commits if this reports a quorum.
func AcceptPhase(leaderID string, activeServers []string, tx shared.Transaction) AcceptResult {
	replies := broadcast(leaderID, activeServers, func(serverID string, client *rpc.Client) (shared.AcceptReply, bool) {
		var reply shared.AcceptReply
		if err := callWithTimeout(client, fmt.Sprintf("Server.%s.AcceptTransactions", serverID), tx, &reply); err != nil {
			return shared.AcceptReply{}, false
		}
		return reply, true
	})

	result := AcceptResult{Accepts: 1, MaxSeen: tx.Ballot} // the leader accepts its own proposal
	for _, reply := range replies {
		if reply.PromisedBallot.GreaterThan(result.MaxSeen) {
			result.MaxSeen = reply.PromisedBallot
		}
		if reply.Accepted {
			result.Accepts++
		}
	}
	result.Quorum = result.Accepts >= shared.GetQuorumSize()
	return result
}

// CommitPhase is best-effort: the decision is already made by the time it runs,
// so a peer that misses it catches up on the next prepare.
func CommitPhase(leaderID string, activeServers []string, tx shared.Transaction) {
	broadcast(leaderID, activeServers, func(serverID string, client *rpc.Client) (struct{}, bool) {
		var reply string
		err := callWithTimeout(client, fmt.Sprintf("Server.%s.CommitTransactions", serverID), tx, &reply)
		return struct{}{}, err == nil
	})
}

// broadcast runs fn against every peer except the leader, in parallel, and
// returns only the replies that came back. Sequential rounds meant one slow
// peer added its latency to every peer after it.
func broadcast[T any](leaderID string, activeServers []string, fn func(string, *rpc.Client) (T, bool)) []T {
	var (
		mu      sync.Mutex
		results []T
		wg      sync.WaitGroup
	)

	for _, serverID := range activeServers {
		if serverID == leaderID {
			continue
		}
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			client, err := dial(serverID)
			if err != nil {
				return
			}
			defer client.Close()

			value, ok := fn(serverID, client)
			if !ok {
				return
			}
			mu.Lock()
			results = append(results, value)
			mu.Unlock()
		}(serverID)
	}

	wg.Wait()
	return results
}

// FetchLongestTransactionHistory is retained for callers that only need a peer
// snapshot outside a Paxos round.
func FetchLongestTransactionHistory(leaderID string, activeServers []string) ([]shared.Transaction, shared.Ballot) {
	type snapshot struct {
		log    []shared.Transaction
		ballot shared.Ballot
	}

	snapshots := broadcast(leaderID, activeServers, func(serverID string, client *rpc.Client) (snapshot, bool) {
		var s snapshot
		if err := callWithTimeout(client, fmt.Sprintf("Server.%s.CommittedTransactionsInDB", serverID), struct{}{}, &s.log); err != nil {
			return snapshot{}, false
		}
		if err := callWithTimeout(client, fmt.Sprintf("Server.%s.FetchBallotNumber", serverID), struct{}{}, &s.ballot); err != nil {
			return snapshot{}, false
		}
		return s, true
	})

	var longest []shared.Transaction
	var highest shared.Ballot
	for _, s := range snapshots {
		if len(s.log) > len(longest) {
			longest = s.log
		}
		if s.ballot.GreaterThan(highest) {
			highest = s.ballot
		}
	}
	return longest, highest
}

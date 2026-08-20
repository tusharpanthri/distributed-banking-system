package server

import (
	"distributed-banking/database"
	"distributed-banking/paxos"
	"distributed-banking/shared"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
)

type TransactionRequest struct {
	Transaction   shared.Transaction
	ActiveServers []string
}

type Server struct {
	ID                   string        // Unique identifier for the server
	Number               int           // Numeric form of ID, used as the ballot tiebreak
	ClusterID            string        // The cluster to which the server belongs
	PromisedBallot       shared.Ballot // Highest ballot this server has promised to
	AcceptedBallot       shared.Ballot // Ballot under which AcceptedLog was accepted
	AcceptedLog          []shared.Transaction
	mu                   sync.Mutex         // Mutex for thread safety
	Database             *database.Database // Database associated with the server
	totalTransactionTime time.Duration      // Total time taken for transactions

	addr       string // Address this server listens on, kept so it can restart
	rpcServer  *rpc.Server
	listenerMu sync.Mutex
	listener   net.Listener // nil while the server is crashed
}

func (s *Server) HandleTransaction(request TransactionRequest, reply *string) error {
	startTime := time.Now() // Start measuring transaction processing time
	s.mu.Lock()
	defer s.mu.Unlock()

	// Propose under a ballot strictly above anything we have seen.
	localLog, err := database.GetAllTransactions(s.Database.DB)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to read local log on server %s: %v", s.ID, err)
	}

	ballot := s.PromisedBallot.Next(s.Number)
	s.PromisedBallot = ballot

	// Prepare Phase: a majority must promise before we may act as leader.
	prepare := paxos.PreparePhase(s.ID, request.ActiveServers, localLog, ballot)
	if !prepare.Quorum {
		// Without a quorum of promises, committing would be a unilateral
		// decision by a leader the cluster has not agreed to follow.
		s.PromisedBallot = prepare.MaxSeen
		*reply = "no quorum for prepare"
		return nil
	}

	// Paxos requires re-proposing the highest-ballot value already accepted in
	// the quorum rather than our own, so an in-flight value is never lost.
	if err := s.UpdateCommittedTransactionsInDB(prepare.AdoptedLog); err != nil {
		return fmt.Errorf("[ERROR] Failed to update committed transactions in DB: %v", err)
	}

	// Check and lock clients
	if err := s.CheckAndLockClients(request.Transaction); err != nil {
		*reply = "transaction aborted"
		return nil
	}

	// Accept Phase: the value is only chosen once a majority accepts it.
	proposal := request.Transaction
	proposal.Ballot = ballot
	accept := paxos.AcceptPhase(s.ID, request.ActiveServers, proposal)
	if !accept.Quorum {
		s.PromisedBallot = accept.MaxSeen
		s.ReleaseLocks(proposal)
		*reply = "no quorum for accept"
		return nil
	}

	s.AcceptedBallot = ballot
	s.AcceptedLog = append(s.AcceptedLog, proposal)

	// Commit transaction locally
	if err := s.CommitTransactionInDB(proposal); err != nil {
		return fmt.Errorf("[ERROR] Failed to commit transaction locally: %v", err)
	}

	// Commit Phase
	paxos.CommitPhase(s.ID, request.ActiveServers, proposal)
	s.totalTransactionTime += time.Since(startTime)
	*reply = "Transaction successfully handled by the leader."
	return nil
}

func (s *Server) Prepare(request shared.PrepareRequest, reply *shared.PromiseReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// An acceptor may only move to a higher ballot. Blindly adopting whatever
	// the caller sent lets a stale leader claw back a cluster that has already
	// promised to someone newer, which is exactly the case Paxos exists to
	// prevent. Reject and report the promise we are already holding so the
	// caller can catch up.
	if !request.LeaderBallot.GreaterThan(s.PromisedBallot) {
		*reply = shared.PromiseReply{
			ServerID:       s.ID,
			Promised:       false,
			PromisedBallot: s.PromisedBallot,
			AcceptedBallot: s.AcceptedBallot,
			AcceptedLog:    s.AcceptedLog,
		}
		return nil
	}

	// Update local database with missing transactions
	err := s.UpdateCommittedTransactionsInDB(request.CommittedTransactions)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to update committed transactions in DB: %v", err)
	}
	s.PromisedBallot = request.LeaderBallot
	// Promise, and report back whatever we have already accepted so the leader
	// can adopt the highest-ballot value in flight rather than overwriting it.
	*reply = shared.PromiseReply{
		ServerID:       s.ID,
		Promised:       true,
		PromisedBallot: s.PromisedBallot,
		AcceptedBallot: s.AcceptedBallot,
		AcceptedLog:    s.AcceptedLog,
	}
	return nil
}

func (s *Server) AcceptTransactions(request shared.Transaction, reply *shared.AcceptReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Refuse any proposal that does not honour our outstanding promise.
	if !request.Ballot.AtLeast(s.PromisedBallot) {
		*reply = shared.AcceptReply{ServerID: s.ID, Accepted: false, PromisedBallot: s.PromisedBallot}
		return nil
	}

	// Check and lock clients
	if err := s.CheckAndLockClients(request); err != nil {
		*reply = shared.AcceptReply{ServerID: s.ID, Accepted: false, PromisedBallot: s.PromisedBallot}
		return fmt.Errorf("transaction aborted: %v", err)
	}

	s.PromisedBallot = request.Ballot
	s.AcceptedBallot = request.Ballot
	s.AcceptedLog = append(s.AcceptedLog, request)
	*reply = shared.AcceptReply{ServerID: s.ID, Accepted: true, PromisedBallot: s.PromisedBallot}
	return nil
}

func (s *Server) CommitTransactions(request shared.Transaction, reply *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Commit transaction locally
	if err := s.CommitTransactionInDB(request); err != nil {
		return fmt.Errorf("[ERROR] Failed to commit transaction locally: %v", err)
	}

	*reply = fmt.Sprintf("Transaction committed: %d -> %d, Amount: %d", request.Source, request.Destination, request.Amount)
	return nil
}

// StartServerRPC brings a replica up on its own RPC server and listener. Each
// replica gets a private rpc.Server rather than the process-wide default, so a
// cluster can be started, stopped, and started again in the same process --
// which is what makes the integration tests possible.
func StartServerRPC(serverID string, port string, clusterID string, shardIDs []int) *Server {
	db, err := database.InitDatabase(serverID, shardIDs)
	if err != nil {
		return nil
	}
	number, err := shared.ServerNumber(serverID)
	if err != nil {
		return nil
	}
	s := &Server{
		ID:        serverID,
		Number:    number,
		ClusterID: clusterID,
		Database:  db,
		addr:      port,
		rpcServer: rpc.NewServer(),
	}
	if err := s.rpcServer.RegisterName(fmt.Sprintf("Server.%s", serverID), s); err != nil {
		return nil
	}
	if err := s.Restore(); err != nil {
		return nil
	}
	return s
}

// Restore starts (or restarts) the listener. A recovered server serves stale
// state until the next prepare round brings it back up to date.
func (s *Server) Restore() error {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	if s.listener != nil {
		return nil
	}
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	go s.serve(listener)
	return nil
}

// Crash simulates the process dying: the listener closes, so peers get a
// connection refused exactly as they would from a dead machine. State on disk
// survives, so Restore brings the replica back with its log intact.
func (s *Server) Crash() error {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	if s.listener == nil {
		return nil
	}
	err := s.listener.Close()
	s.listener = nil
	return err
}

// IsRunning reports whether this replica is currently accepting connections.
func (s *Server) IsRunning() bool {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	return s.listener != nil
}

// Stop crashes the server and releases its database handle.
func (s *Server) Stop() error {
	if err := s.Crash(); err != nil {
		return err
	}
	return s.Database.DB.Close()
}

func (s *Server) serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			// The listener was closed by Crash or Stop; the previous code
			// continued here and spun the CPU on a permanently failing Accept.
			return
		}
		go s.rpcServer.ServeConn(conn)
	}
}

func (s *Server) CommittedTransactionsInDB(_ struct{}, reply *[]shared.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	transactions, err := database.GetAllTransactions(s.Database.DB)
	if err != nil {
		return fmt.Errorf("failed to fetch transactions from DB for server %s: %v", s.ID, err)
	}

	*reply = transactions
	return nil
}

// UpdateCommittedTransactionsInDB reconciles the local log against the log the
// leader adopted from its prepare quorum.
//
// The previous version compared logs by row count, and if the remote was longer
// it DELETEd every local row and rewrote the table from the remote copy. That
// is unsound twice over: length is not authority, so a replica that happened to
// hold more rows could overwrite entries agreed under a higher ballot; and
// destroying committed state to synchronise means a single bad peer can erase
// this server's durable history.
//
// This version only ever adds. Entries already present are left alone, and
// nothing is deleted.
func (s *Server) UpdateCommittedTransactionsInDB(adoptedLog []shared.Transaction) error {
	if len(adoptedLog) == 0 {
		return nil
	}

	localTransactions, err := database.GetAllTransactions(s.Database.DB)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to fetch local transactions for server %s: %v", s.ID, err)
	}

	local := make(map[string]shared.Transaction, len(localTransactions))
	for _, tx := range localTransactions {
		local[tx.TransactionID] = tx
	}

	for _, tx := range adoptedLog {
		if _, exists := local[tx.TransactionID]; exists {
			continue
		}

		// Prepared cross-shard entries are recorded but not applied: their
		// balance effect lands with the 2PC decision, not here.
		if tx.Status != "P" {
			if err := database.UpdateClientBalance(s.Database.DB, tx.Source, -tx.Amount); err != nil {
				return fmt.Errorf("[ERROR] Failed to update balance for Sender %d: %v", tx.Source, err)
			}
			if err := database.UpdateClientBalance(s.Database.DB, tx.Destination, tx.Amount); err != nil {
				return fmt.Errorf("[ERROR] Failed to update balance for Receiver %d: %v", tx.Destination, err)
			}
		}

		if err := database.AddTransaction(s.Database.DB, tx.TransactionID, tx.Source, tx.Destination,
			tx.Amount, tx.Ballot, tx.ContactServer, tx.Status); err != nil {
			return fmt.Errorf("[ERROR] Failed to add transaction %s to DB for server %s: %v", tx.TransactionID, s.ID, err)
		}
		local[tx.TransactionID] = tx
	}

	return nil
}

func (s *Server) CheckAndLockClients(transaction shared.Transaction) error {
	senderLocked, err := database.IsLocked(s.Database.DB, transaction.Source)
	if err != nil || senderLocked {
		return fmt.Errorf("[DEBUG] sender %d is locked or error occurred", transaction.Source)
	}
	receiverLocked, err := database.IsLocked(s.Database.DB, transaction.Destination)
	if err != nil || receiverLocked {
		return fmt.Errorf("[DEBUG] receiver %d is locked or error occurred", transaction.Destination)
	}
	senderBalance, err := database.GetClientBalance(s.Database.DB, transaction.Source)
	if err != nil || senderBalance < transaction.Amount {
		return fmt.Errorf("[DEBUG] insufficient balance for Sender %d", transaction.Source)
	}
	if err := database.SetLock(s.Database.DB, transaction.Source); err != nil {
		return fmt.Errorf("failed to lock Sender %d: %v", transaction.Source, err)
	}
	if err := database.SetLock(s.Database.DB, transaction.Destination); err != nil {
		return fmt.Errorf("failed to lock Receiver %d: %v", transaction.Destination, err)
	}
	return nil
}

// ReleaseLocks undoes the locks taken by CheckAndLockClients when a proposal
// fails to reach a quorum. Without this a failed round leaves both accounts
// locked forever and every later transaction touching them aborts.
func (s *Server) ReleaseLocks(transaction shared.Transaction) {
	_ = database.UnsetLock(s.Database.DB, transaction.Source)
	_ = database.UnsetLock(s.Database.DB, transaction.Destination)
}

// ReleaseLocksForRole releases only the account this shard is responsible for
// in a cross-shard transaction.
func (s *Server) ReleaseLocksForRole(transaction shared.Transaction, role string) {
	switch role {
	case "source":
		_ = database.UnsetLock(s.Database.DB, transaction.Source)
	case "destination":
		_ = database.UnsetLock(s.Database.DB, transaction.Destination)
	}
}

func (s *Server) CommitTransactionInDB(transaction shared.Transaction) error {
	if err := database.UpdateClientBalance(s.Database.DB, transaction.Source, -transaction.Amount); err != nil {
		return fmt.Errorf("failed to update balance for Sender %d: %v", transaction.Source, err)
	}
	if err := database.UpdateClientBalance(s.Database.DB, transaction.Destination, transaction.Amount); err != nil {
		return fmt.Errorf("failed to update balance for Receiver %d: %v", transaction.Destination, err)
	}
	if err := database.AddTransaction(s.Database.DB, transaction.TransactionID,
		transaction.Source, transaction.Destination,
		transaction.Amount, s.PromisedBallot, transaction.ContactServer, transaction.Status); err != nil {
		return fmt.Errorf("failed to add committed transaction: %v", err)
	}
	if err := database.UnsetLock(s.Database.DB, transaction.Source); err != nil {
		return fmt.Errorf("failed to unlock Sender %d: %v", transaction.Source, err)
	}
	if err := database.UnsetLock(s.Database.DB, transaction.Destination); err != nil {
		return fmt.Errorf("failed to unlock Receiver %d: %v", transaction.Destination, err)
	}

	return nil
}

// HandleCrossShardTransaction processes transactions that span multiple shards
func (s *Server) HandleCrossShardTransaction(request struct {
	Transaction   shared.Transaction
	ActiveServers []string
	Role          string
}, reply *string) error {
	startTime := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	localLog, err := database.GetAllTransactions(s.Database.DB)
	if err != nil {
		*reply = "failed"
		return fmt.Errorf("[ERROR] Failed to read local log on server %s: %v", s.ID, err)
	}

	ballot := s.PromisedBallot.Next(s.Number)
	s.PromisedBallot = ballot

	// Prepare Phase: this shard must agree before it can cast a 2PC vote.
	prepare := paxos.PreparePhase(s.ID, request.ActiveServers, localLog, ballot)
	if !prepare.Quorum {
		s.PromisedBallot = prepare.MaxSeen
		*reply = "failed"
		return nil
	}

	if err := s.UpdateCommittedTransactionsInDB(prepare.AdoptedLog); err != nil {
		*reply = "failed"
		return fmt.Errorf("[ERROR] Failed to update committed transactions in DB: %v", err)
	}

	// Check locks based on role
	if request.Role == "source" {
		// For source, check balance and lock
		senderLocked, err := database.IsLocked(s.Database.DB, request.Transaction.Source)
		if err != nil || senderLocked {
			*reply = "failed"
			return nil
		}

		senderBalance, err := database.GetClientBalance(s.Database.DB, request.Transaction.Source)
		if err != nil || senderBalance < request.Transaction.Amount {
			*reply = "failed"
			return nil
		}

		if err := database.SetLock(s.Database.DB, request.Transaction.Source); err != nil {
			*reply = "failed"
			return nil
		}
	} else if request.Role == "destination" {
		// For destination, only check lock
		receiverLocked, err := database.IsLocked(s.Database.DB, request.Transaction.Destination)
		if err != nil || receiverLocked {
			*reply = "failed"
			return nil
		}

		if err := database.SetLock(s.Database.DB, request.Transaction.Destination); err != nil {
			*reply = "failed"
			return nil
		}
	}
	// Accept Phase: this shard votes yes only if a majority of it accepts.
	proposal := request.Transaction
	proposal.Ballot = ballot
	accept := paxos.AcceptPhase(s.ID, request.ActiveServers, proposal)
	if !accept.Quorum {
		s.PromisedBallot = accept.MaxSeen
		s.ReleaseLocksForRole(proposal, request.Role)
		*reply = "failed"
		return nil
	}

	s.AcceptedBallot = ballot
	s.AcceptedLog = append(s.AcceptedLog, proposal)

	// Record the prepared entry. Locks stay held until the 2PC decision.
	if err := database.AddTransaction(s.Database.DB, proposal.TransactionID,
		proposal.Source, proposal.Destination,
		proposal.Amount, ballot, proposal.ContactServer, proposal.Status); err != nil {
		*reply = "failed"
		return fmt.Errorf("[ERROR] Failed to add committed transaction: %v", err)
	}

	// Update balance based on role (without removing locks)
	if request.Role == "source" {
		if err := database.UpdateClientBalance(s.Database.DB, request.Transaction.Source,
			-request.Transaction.Amount); err != nil {
			*reply = "failed"
			return fmt.Errorf("failed to update source balance: %v", err)
		}
	} else if request.Role == "destination" {
		if err := database.UpdateClientBalance(s.Database.DB, request.Transaction.Destination,
			request.Transaction.Amount); err != nil {
			*reply = "failed"
			return fmt.Errorf("failed to update destination balance: %v", err)
		}
	}

	// Commit Phase
	paxos.CommitPhase(s.ID, request.ActiveServers, proposal)
	s.totalTransactionTime += time.Since(startTime)
	*reply = "success"
	return nil
}

func (s *Server) Handle2PCCommit(args shared.TwoPCArgs, reply *shared.TwoPCReply) error {
	// Lock the database for thread safety
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if transaction exists and has status "P" (Pending)
	tx, err := database.GetTransaction(s.Database.DB, args.Transaction.TransactionID)
	if err != nil {
		// Return nil if the transaction is not found
		fmt.Printf("[DEBUG] Transaction %s not found\n", args.Transaction.TransactionID)
		return nil
	}

	// If the transaction exists, handle based on the role
	if args.Role == "2PCcommit" {
		// Add transaction to the database and change its status to "C"

		if err := database.AddTransaction(s.Database.DB, uuid.New().String(), tx.Source, tx.Destination, tx.Amount, tx.Ballot, tx.ContactServer, "C"); err != nil {
			return fmt.Errorf("failed to add transaction %s: %v", tx.TransactionID, err)
		}
		// Unset locks
		if err := database.UnsetLock(s.Database.DB, tx.Source); err != nil {
			return fmt.Errorf("failed to unlock Sender %d: %v", tx.Source, err)
		}
		if err := database.UnsetLock(s.Database.DB, tx.Destination); err != nil {
			return fmt.Errorf("failed to unlock Receiver %d: %v", tx.Destination, err)
		}
	} else if args.Role == "2PCabort" {
		// Undo the operation
		if args.CrossShardRole == "source" {
			if err := database.UpdateClientBalance(s.Database.DB, tx.Source, tx.Amount); err != nil {
				return fmt.Errorf("failed to update balance for Sender %d: %v", tx.Source, err)
			}
		} else {
			if err := database.UpdateClientBalance(s.Database.DB, tx.Destination, -tx.Amount); err != nil {
				return fmt.Errorf("failed to update balance for Receiver %d: %v", tx.Destination, err)
			}
		}
		// Unset locks
		if err := database.UnsetLock(s.Database.DB, tx.Source); err != nil {
			return fmt.Errorf("failed to unlock Sender %d: %v", tx.Source, err)
		}
		if err := database.UnsetLock(s.Database.DB, tx.Destination); err != nil {
			return fmt.Errorf("failed to unlock Receiver %d: %v", tx.Destination, err)
		}
	}

	return nil
}

// server.go

func (s *Server) GetBalance(clientID int, reply *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	balance, err := database.GetClientBalance(s.Database.DB, clientID)
	if err != nil {
		return fmt.Errorf("failed to get balance for client %d: %v", clientID, err)
	}
	*reply = balance
	return nil
}

func (s *Server) FetchBallotNumber(_ struct{}, reply *shared.Ballot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	*reply = s.PromisedBallot
	return nil
}

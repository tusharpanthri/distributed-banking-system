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
	ID                   string             // Unique identifier for the server
	ClusterID            string             // The cluster to which the server belongs
	BallotNumber         int                // Ballot number for Paxos protocol
	mu                   sync.Mutex         // Mutex for thread safety
	Database             *database.Database // Database associated with the server
	totalTransactionTime time.Duration      // Total time taken for transactions
}

func (s *Server) HandleTransaction(request TransactionRequest, reply *string) error {
	startTime := time.Now() // Start measuring transaction processing time
	s.mu.Lock()
	defer s.mu.Unlock()

	// fmt.Printf("[DEBUG] Handling transaction on server %s: %v\n", s.ID, request.Transaction)

	// Fetch the longest transaction history
	longestHistory, longestBallotNumber := paxos.FetchLongestTransactionHistory(s.ID, request.ActiveServers)
	// fmt.Printf("[DEBUG] Longest history fetched: %v\n", longestHistory)

	// Check if longestHistory is empty
	if len(longestHistory) == 0 {
		fmt.Printf("[WARNING] Longest history is empty on server %s\n", s.ID)
	}
	if s.BallotNumber < longestBallotNumber {
		s.BallotNumber = longestBallotNumber
	}
	// Update local database with missing transactions
	// fmt.Printf("[DEBUG] Attempting to update committed transactions in DB for server %s\n", s.ID)
	if err := s.UpdateCommittedTransactionsInDB(longestHistory); err != nil {
		return fmt.Errorf("[ERROR] Failed to update committed transactions in DB: %v", err)
	}
	// fmt.Printf("[DEBUG] Successfully updated committed transactions in DB for server %s\n", s.ID)

	// Prepare Phase

	s.BallotNumber++
	// fmt.Printf("[DEBUG] Ballot number is increased to %d on server %s\n", s.BallotNumber, s.ID)
	// fmt.Printf("[DEBUG] Starting Prepare Phase on server %s\n", s.ID)
	paxos.PreparePhase(s.ID, request.ActiveServers, longestHistory, s.BallotNumber)

	// Add quorum check
	if len(request.ActiveServers) < shared.GetQuorumSize() {
		// fmt.Printf("[WARNING] Not enough active servers for quorum (minimum %d required, got %d). Skipping transaction.\n", shared.GetQuorumSize(), len(request.ActiveServers))
		return nil
	}

	// Check and lock clients
	// fmt.Printf("[DEBUG] Checking and locking clients for transaction on server %s\n", s.ID)
	if err := s.CheckAndLockClients(request.Transaction); err != nil {
		// fmt.Printf("[DEBUG] Transaction aborted: %v\n", err)
		return nil
	}

	// Accept Phase
	// fmt.Printf("[DEBUG] Starting Accept Phase on server %s\n", s.ID)
	paxos.AcceptPhase(s.ID, request.ActiveServers, request.Transaction)

	// Commit transaction locally
	// fmt.Printf("[DEBUG] Committing transaction locally on server %s\n", s.ID)
	if err := s.CommitTransactionInDB(request.Transaction); err != nil {
		return fmt.Errorf("[ERROR] Failed to commit transaction locally: %v", err)
	}

	// Commit Phase
	// fmt.Printf("[DEBUG] Starting Commit Phase on server %s\n", s.ID)
	paxos.CommitPhase(s.ID, request.ActiveServers, request.Transaction)
	s.totalTransactionTime += time.Since(startTime)
	*reply = "Transaction successfully handled by the leader."
	// fmt.Printf("[DEBUG] Transaction handled successfully on server %s\n", s.ID)
	return nil
}

func (s *Server) Prepare(request shared.PrepareRequest, reply *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// fmt.Printf("[DEBUG] Prepare request received on server %s\n", s.ID)

	// Update local database with missing transactions
	err := s.UpdateCommittedTransactionsInDB(request.CommittedTransactions)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to update committed transactions in DB: %v", err)
	}
	s.BallotNumber = request.LeaderBallotNumber
	// fmt.Printf("[DEBUG] Ballot number is increased to %d on server %s\n", s.BallotNumber, s.ID)
	// Acknowledge the prepare phase request
	*reply = fmt.Sprintf("Server %s is prepared", s.ID)
	// fmt.Printf("[DEBUG] Prepare phase completed on server %s\n", s.ID)
	return nil
}

func (s *Server) AcceptTransactions(request shared.Transaction, reply *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// fmt.Printf("[DEBUG] Accepting transaction on server %s: %v\n", s.ID, request)

	// Check and lock clients
	if err := s.CheckAndLockClients(request); err != nil {
		return fmt.Errorf("transaction aborted: %v", err)
	}
	*reply = fmt.Sprintf("Transaction completed: %d -> %d, Amount: %d", request.Source, request.Destination, request.Amount)
	// fmt.Printf("[DEBUG] Transaction completed on server %s: %v\n", s.ID, request)
	return nil
}

func (s *Server) CommitTransactions(request shared.Transaction, reply *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// fmt.Printf("[DEBUG] Committing transaction on server %s: %v\n", s.ID, request)

	// Commit transaction locally
	if err := s.CommitTransactionInDB(request); err != nil {
		return fmt.Errorf("[ERROR] Failed to commit transaction locally: %v", err)
	}

	*reply = fmt.Sprintf("Transaction committed: %d -> %d, Amount: %d", request.Source, request.Destination, request.Amount)
	// fmt.Printf("[DEBUG] Transaction committed on server %s: %v\n", s.ID, request)
	return nil
}
func StartServerRPC(serverID string, port string, clusterID string, shardIDs []int) *Server {
	db, err := database.InitDatabase(serverID, shardIDs)
	if err != nil {
		// fmt.Printf("Error initializing database for server %s: %v\n", serverID, err)
		return nil
	}
	s := &Server{
		ID:           serverID,
		ClusterID:    clusterID,
		Database:     db, // Assign the database instance
		BallotNumber: 0,
	}
	err = rpc.RegisterName(fmt.Sprintf("Server.%s", serverID), s)
	if err != nil {
		// fmt.Printf("Error registering server %s: %v\n", serverID, err)
		return nil
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		// fmt.Println("Error starting server:", err)
		return nil
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// fmt.Printf("Error accepting connection on server %s: %v\n", serverID, err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
	return s
}

func (s *Server) CommittedTransactionsInDB(_ struct{}, reply *[]shared.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	transactions, err := database.GetAllTransactions(s.Database.DB)
	if err != nil {
		// return fmt.Errorf("failed to fetch transactions from DB for server %s: %v", s.ID, err)
	}
	var committedTransactions []shared.Transaction
	for _, tx := range transactions {
		committedTransactions = append(committedTransactions, shared.Transaction{
			TransactionID: tx["transaction_id"].(string),
			Source:        tx["source"].(int),
			Destination:   tx["destination"].(int),
			Amount:        tx["amount"].(int),
			BallotNumber:  tx["ballot_number"].(int),
			ContactServer: tx["contact_server"].(int),
			Status:        tx["status"].(string),
		})
	}

	*reply = committedTransactions
	return nil
}

func (s *Server) UpdateCommittedTransactionsInDB(longestHistory []shared.Transaction) error {
	// fmt.Printf("[DEBUG] Inside UpdateCommittedTransactionsInDB for server %s\n", s.ID)
	// fmt.Printf("[DEBUG] Longest History: %v\n", longestHistory)

	// Fetch all local transactions from the database
	localTransactions, err := database.GetAllTransactions(s.Database.DB)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to fetch local transactions for server %s: %v", s.ID, err)
	}
	if len(localTransactions) >= len(longestHistory) {
		return nil
	}

	// Create a map for efficient lookup of local transactions by TransactionID
	localTransactionMap := make(map[string]shared.Transaction)
	for _, tx := range localTransactions {
		localTransactionMap[tx["transaction_id"].(string)] = shared.Transaction{
			TransactionID: tx["transaction_id"].(string),
			Source:        tx["source"].(int),
			Destination:   tx["destination"].(int),
			Amount:        tx["amount"].(int),
			BallotNumber:  tx["ballot_number"].(int),
			ContactServer: tx["contact_server"].(int),
			Status:        tx["status"].(string),
		}
	}

	// Identify missing transactions in the local database
	var missingTransactions []shared.Transaction
	// maxBallotNumber := s.BallotNumber // Initialize with the current server ballot number
	for _, tx := range longestHistory {
		if _, exists := localTransactionMap[tx.TransactionID]; !exists {
			if tx.Status != "P" {
				missingTransactions = append(missingTransactions, tx)
			}
		}
		// Update maxBallotNumber
		// if tx.BallotNumber > maxBallotNumber {
		// 	maxBallotNumber = tx.BallotNumber
		// }
	}
	// Update the server's ballot number to the highest found
	// Update client balances for missing transactions
	for _, tx := range missingTransactions {
		// fmt.Printf("[DEBUG] Updating balances for missing transaction %s on server %s\n", tx.TransactionID, s.ID)

		// Deduct from the source client
		if err := database.UpdateClientBalance(s.Database.DB, tx.Source, -tx.Amount); err != nil {
			return fmt.Errorf("[ERROR] Failed to update balance for Sender %d: %v", tx.Source, err)
		}

		// Add to the destination client
		if err := database.UpdateClientBalance(s.Database.DB, tx.Destination, tx.Amount); err != nil {
			return fmt.Errorf("[ERROR] Failed to update balance for Receiver %d: %v", tx.Destination, err)
		}
	}

	// Clear all existing transactions
	if err := database.ClearTransactions(s.Database.DB); err != nil {
		return fmt.Errorf("[ERROR] Failed to clear transactions for server %s: %v", s.ID, err)
	}

	// Add all transactions from longestHistory in order
	for _, tx := range longestHistory {
		// fmt.Printf("[DEBUG] Adding transaction %s to DB on server %s\n", tx.TransactionID, s.ID)
		err := database.AddTransaction(s.Database.DB, tx.TransactionID, tx.Source, tx.Destination, tx.Amount, tx.BallotNumber, tx.ContactServer, tx.Status)
		if err != nil {
			return fmt.Errorf("[ERROR] Failed to add transaction %s to DB for server %s: %v", tx.TransactionID, s.ID, err)
		}
	}

	// fmt.Printf("[DEBUG] Completed updating transactions in DB for server %s\n", s.ID)
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

func (s *Server) CommitTransactionInDB(transaction shared.Transaction) error {
	if err := database.UpdateClientBalance(s.Database.DB, transaction.Source, -transaction.Amount); err != nil {
		return fmt.Errorf("failed to update balance for Sender %d: %v", transaction.Source, err)
	}
	if err := database.UpdateClientBalance(s.Database.DB, transaction.Destination, transaction.Amount); err != nil {
		return fmt.Errorf("failed to update balance for Receiver %d: %v", transaction.Destination, err)
	}
	if err := database.AddTransaction(s.Database.DB, transaction.TransactionID,
		transaction.Source, transaction.Destination,
		transaction.Amount, s.BallotNumber, transaction.ContactServer, transaction.Status); err != nil {
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

	// fmt.Printf("[DEBUG] Handling cross-shard transaction on server %s with role %s: %v\n", s.ID, request.Role, request.Transaction)

	// Fetch the longest transaction history
	longestHistory, longestBallotNumber := paxos.FetchLongestTransactionHistory(s.ID, request.ActiveServers)
	// fmt.Printf("[DEBUG] Longest history fetched: %v\n", longestHistory)
	if s.BallotNumber < longestBallotNumber {
		s.BallotNumber = longestBallotNumber
	}
	// Update local database with missing transactions
	if err := s.UpdateCommittedTransactionsInDB(longestHistory); err != nil {
		*reply = "failed"
		return fmt.Errorf("[ERROR] Failed to update committed transactions in DB: %v", err)
	}

	// Prepare Phase
	s.BallotNumber++
	// fmt.Printf("[DEBUG] Ballot number is increased to %d on server %s\n", s.BallotNumber, s.ID)
	paxos.PreparePhase(s.ID, request.ActiveServers, longestHistory, s.BallotNumber)

	// Add quorum check
	if len(request.ActiveServers) < shared.GetQuorumSize() {
		*reply = "failed"
		// fmt.Printf("[WARNING] Not enough active servers for quorum (minimum %d required, got %d)\n", shared.GetQuorumSize(), len(request.ActiveServers))
		return nil
	}

	// Check locks based on role
	if request.Role == "source" {
		// For source, check balance and lock
		senderLocked, err := database.IsLocked(s.Database.DB, request.Transaction.Source)
		if err != nil || senderLocked {
			// fmt.Printf("[DEBUG] Sender %d is locked, aborting transaction\n", request.Transaction.Source)
			*reply = "failed"
			return nil
		}

		senderBalance, err := database.GetClientBalance(s.Database.DB, request.Transaction.Source)
		if err != nil || senderBalance < request.Transaction.Amount {
			// fmt.Printf("[DEBUG] Insufficient balance for Sender %d\n", request.Transaction.Source)
			*reply = "failed"
			return nil
		}

		if err := database.SetLock(s.Database.DB, request.Transaction.Source); err != nil {
			*reply = "failed"
			// fmt.Printf("[DEBUG] Failed to lock Sender %d: %v\n", request.Transaction.Source, err)
			return nil
		}
	} else if request.Role == "destination" {
		// For destination, only check lock
		receiverLocked, err := database.IsLocked(s.Database.DB, request.Transaction.Destination)
		if err != nil || receiverLocked {
			*reply = "failed"
			// fmt.Printf("[DEBUG] Receiver %d is locked or error occurred\n", request.Transaction.Destination)
			return nil
		}

		if err := database.SetLock(s.Database.DB, request.Transaction.Destination); err != nil {
			*reply = "failed"
			// fmt.Printf("[DEBUG] Failed to lock Receiver %d: %v\n", request.Transaction.Destination, err)
			return nil
		}
	}
	// Accept Phase
	paxos.AcceptPhase(s.ID, request.ActiveServers, request.Transaction)

	// Update transaction status to committed
	if err := database.AddTransaction(s.Database.DB, request.Transaction.TransactionID,
		request.Transaction.Source, request.Transaction.Destination,
		request.Transaction.Amount, s.BallotNumber, request.Transaction.ContactServer, request.Transaction.Status); err != nil {
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
	paxos.CommitPhase(s.ID, request.ActiveServers, request.Transaction)
	s.totalTransactionTime += time.Since(startTime)
	*reply = "success"
	// fmt.Printf("[DEBUG] Cross-shard transaction handled successfully on server %s\n", s.ID)
	return nil
}

func (s *Server) Handle2PCCommit(args shared.TwoPCArgs, reply *shared.TwoPCReply) error {
	// Lock the database for thread safety
	s.mu.Lock()
	defer s.mu.Unlock()

	// fmt.Printf("[DEBUG] Handling 2PC commit on server %s with role %s and crossShardRole %s\n", s.ID, args.Role, args.CrossShardRole)
	// Check if transaction exists and has status "P" (Pending)
	tx, err := database.GetTransaction(s.Database.DB, args.Transaction.TransactionID)
	if err != nil {
		// Return nil if the transaction is not found
		fmt.Printf("[DEBUG] Transaction %s not found\n", args.Transaction.TransactionID)
		return nil
	}

	// If the transaction exists, handle based on the role
	if args.Role == "2PCcommit" {
		// fmt.Printf("[DEBUG] Received 2PC commit for transaction %s\n", tx.TransactionID)
		// Add transaction to the database and change its status to "C"

		if err := database.AddTransaction(s.Database.DB, uuid.New().String(), tx.Source, tx.Destination, tx.Amount, tx.BallotNumber, tx.ContactServer, "C"); err != nil {
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
		// fmt.Printf("[DEBUG] Received 2PC abort for transaction %s\n", tx.TransactionID)
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

func (s *Server) FetchBallotNumber(_ struct{}, reply *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	*reply = s.BallotNumber
	return nil
}

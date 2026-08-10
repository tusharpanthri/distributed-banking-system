package paxos

import (
	"distributed-banking/shared"
	"fmt"
	"net/rpc"
)

func FetchLongestTransactionHistory(leaderID string, activeServers []string) ([]shared.Transaction, int) {
	var longestHistory []shared.Transaction
	var highestBallotNumber int
	maxLength := 0

	for _, serverID := range activeServers {
		if serverID == leaderID {
			continue
		}

		serverAddr, ok := shared.ServerAddresses(serverID)
		if ok != nil {
			// fmt.Printf("Server address not found for server %s\n", serverID)
			continue
		}

		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			// fmt.Printf("Failed to connect to server %s: %v\n", serverAddr, err)
			continue
		}
		defer client.Close()

		var serverTransactions []shared.Transaction
		err = client.Call(fmt.Sprintf("Server.%s.CommittedTransactionsInDB", serverID), struct{}{}, &serverTransactions)
		if err != nil {
			// fmt.Printf("Failed to fetch committed transactions from server %s: %v\n", serverID, err)
			continue
		}
		if len(serverTransactions) > maxLength {
			longestHistory = serverTransactions
			maxLength = len(serverTransactions)
		}
		var ballotNumber int
		err = client.Call(fmt.Sprintf("Server.%s.FetchBallotNumber", serverID), struct{}{}, &ballotNumber)
		if err != nil {
			// fmt.Printf("Failed to fetch committed transactions from server %s: %v\n", serverID, err)
			continue
		}
		if ballotNumber > highestBallotNumber {
			highestBallotNumber = ballotNumber
		}
	}

	return longestHistory, highestBallotNumber
}

func PreparePhase(leaderID string, activeServers []string, longestHistory []shared.Transaction, leaderBallotNumber int) {
	for _, serverID := range activeServers {
		if serverID == leaderID {
			continue
		}

		serverAddr, ok := shared.ServerAddresses(serverID)
		if ok != nil {
			// fmt.Printf("Server address not found for server %s\n", serverID)
			continue
		}

		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			// fmt.Printf("Failed to connect to server %s: %v\n", serverAddr, err)
			continue
		}
		defer client.Close()

		// Send prepare request with the longest history
		var acknowledgment string
		prepareRequest := shared.PrepareRequest{
			CommittedTransactions: longestHistory,
			LeaderBallotNumber:    leaderBallotNumber,
		}

		err = client.Call(fmt.Sprintf("Server.%s.Prepare", serverID), prepareRequest, &acknowledgment)
		if err != nil {
			// fmt.Printf("Failed to prepare phase on server %s: %v\n", serverID, err)
		} else {
			// fmt.Printf("Prepare phase acknowledgment received from server %s: %s\n", serverID, acknowledgment)
		}
	}
}

func AcceptPhase(leaderID string, activeServers []string, singleTransaction shared.Transaction) {
	for _, serverID := range activeServers {
		if serverID == leaderID {
			continue
		}

		serverAddr, ok := shared.ServerAddresses(serverID)
		if ok != nil {
			// fmt.Printf("Server address not found for server %s\n", serverID)
			continue
		}

		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			// fmt.Printf("Failed to connect to server %s: %v\n", serverAddr, err)
			continue
		}
		defer client.Close()

		var reply string
		err = client.Call(fmt.Sprintf("Server.%s.AcceptTransactions", serverID), singleTransaction, &reply)
		if err != nil {
			// fmt.Printf("Failed to x transaction to server %s: %v\n", serverID, err)
		} else {
			// fmt.Printf("Accept Phase successful on server %s: %s\n", serverID, reply)
		}
	}
}

func CommitPhase(leaderID string, activeServers []string, transaction shared.Transaction) {
	for _, serverID := range activeServers {
		if serverID == leaderID {
			continue
		}

		serverAddr, ok := shared.ServerAddresses(serverID)
		if ok != nil {
			// fmt.Printf("Server address not found for server %s\n", serverID)
			continue
		}

		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			// fmt.Printf("Failed to connect to server %s: %v\n", serverAddr, err)
			continue
		}
		defer client.Close()

		var reply string
		err = client.Call(fmt.Sprintf("Server.%s.CommitTransactions", serverID), transaction, &reply)
		if err != nil {
			// fmt.Printf("Failed to commit transaction on server %s: %v\n", serverID, err)
		} else {
			// fmt.Printf("Commit Phase successful on server %s: %s\n", serverID, reply)
		}
	}
}

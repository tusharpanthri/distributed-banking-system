package client

import (
	"distributed-banking/shared"
	"fmt"
	"net/rpc"
	"time"
)

func ConnectToServer(serverName string) *rpc.Client {
	serverAddress, ok := shared.ServerAddresses(serverName)
	if ok != nil {
		// fmt.Printf("Server address not found for server %s\n", serverName)
	}
	if serverAddress == "" {
		// fmt.Printf("Error: Server address for %s is missing\n", serverName)
		return nil
	}

	client, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		// fmt.Printf("Error connecting to server %s at %s: %v\n", serverName, serverAddress, err)
		return nil
	}
	return client
}

// client.go

func SendIntraShardTransaction(serverName string, tx shared.Transaction, activeServers []string) (bool, time.Duration) {
	startTime := time.Now()
	client := ConnectToServer(serverName)
	if client == nil {
		// fmt.Println("Failed to connect to server, transaction aborted.")
		return false, time.Since(startTime)
	}
	defer client.Close()

	type TransactionRequest struct {
		Transaction   shared.Transaction
		ActiveServers []string
	}

	request := TransactionRequest{
		Transaction:   tx,
		ActiveServers: activeServers,
	}

	var reply string
	err := client.Call(fmt.Sprintf("Server.%s.HandleTransaction", serverName), request, &reply)
	if err != nil {
		// fmt.Println("Transaction error:", err)
		return false, time.Since(startTime)
	}
	elapsedTime := time.Since(startTime)
	return true, elapsedTime
}

func SendCrossShardTransaction(serverName string, tx shared.Transaction, activeServers []string, role string) (bool, time.Duration) {
	startTime := time.Now()
	client := ConnectToServer(serverName)
	if client == nil {
		// fmt.Println("Failed to connect to server, transaction aborted.")
		return false, time.Since(startTime)
	}
	defer client.Close()

	type TransactionRequest struct {
		Transaction   shared.Transaction
		ActiveServers []string
		Role          string // Indicates whether the server is "source" or "destination"
	}

	request := TransactionRequest{
		Transaction:   tx,
		ActiveServers: activeServers,
		Role:          role,
	}

	var reply string
	err := client.Call(fmt.Sprintf("Server.%s.HandleCrossShardTransaction", serverName), request, &reply)
	if err != nil {
		// fmt.Printf("Cross-shard transaction error on server %s (%s): %v\n", serverName, role, err)
		return false, time.Since(startTime)
	}

	// fmt.Printf("Cross-shard transaction response from server %s (%s): %s\n", serverName, role, reply)
	elapsedTime := time.Since(startTime)
	return reply == "success", elapsedTime
}

// Send2PCCommit sends a 2PC commit/abort message to the specified server
func Send2PCCommit(serverID string, transaction shared.Transaction, role string, crossShardRole string) bool {
	// fmt.Printf("Debug: Sending 2PC commit to server %s with role %s and crossShardRole %s\n", serverID, role, crossShardRole)
	client := ConnectToServer(serverID)
	if client == nil {
		// fmt.Printf("Error connecting to server %s\n", serverID)
		return false
	}
	defer client.Close()

	// Prepare arguments for the RPC call
	args := shared.TwoPCArgs{
		Transaction:    transaction,
		Role:           role,
		CrossShardRole: crossShardRole,
	}
	var reply shared.TwoPCReply

	// Make the RPC call
	err := client.Call(fmt.Sprintf("Server.%s.Handle2PCCommit", serverID), args, &reply)
	if err != nil {
		// fmt.Printf("Error in 2PC call to server %s: %v\n", serverID, err)
		return false
	}

	return reply.Success
}

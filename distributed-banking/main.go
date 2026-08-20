package main

import (
	"bufio"
	"distributed-banking/client"
	"distributed-banking/csv_parser"
	"distributed-banking/server"
	"distributed-banking/shared"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type PendingCommit struct {
	sequence      int
	transaction   shared.Transaction
	sourceServers []string
	destServers   []string
	shouldCommit  bool
}

func main() {
	// Configure clusters dynamically
	clusterCount, serversPerCluster := ConfigureClusters()

	// Set the servers per cluster in shared package for quorum calculations
	shared.SetServersPerCluster(serversPerCluster)

	// Initialize clusters and servers
	clusterServers := InitializeClusters(clusterCount, serversPerCluster)
	fmt.Println("Clusters initialized:", clusterServers)

	// Initialize the server-to-cluster mapping
	shared.InitializeServerToClusterMapping(clusterServers)
	fmt.Println("Server-to-cluster mapping initialized.")

	// Assign data shards to clusters
	shardMapping := AssignShardsToClusters(3000, clusterCount)

	// Start servers using StartServerRPC. Keep the handles: they are what let
	// the operator crash and restore a replica from the menu.
	servers := make(map[string]*server.Server)
	for clusterID, serverIDs := range clusterServers {
		for _, serverID := range serverIDs {
			shardsForCluster := GetShardsForCluster(shardMapping, clusterID)
			port, ok := shared.ServerAddresses(serverID) // Retrieve port from shared.ServerAddresses
			if ok != nil {
				fmt.Printf("Server address not found for server %s\n", serverID)
				continue
			}
			srv := server.StartServerRPC(serverID, port, clusterID, shardsForCluster)
			if srv == nil {
				fmt.Printf("Error starting server %s\n", serverID)
				continue
			}
			servers[serverID] = srv
		}
	}

	// Parse CSV for transaction sets
	sets, err := csv_parser.ParseCSV("New_Test_Cases_-_Lab3.csv")
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}
	fmt.Println("Starting Distributed Banking System...")
	var totalTransactions int
	var totalTime time.Duration
	// Process transactions
	reader := bufio.NewReader(os.Stdin)
	pendingCommits := []PendingCommit{}
	sequenceCounter := 0
	for _, set := range sets {
		fmt.Printf("Processing Set %d\n", set.SetNumber)
		fmt.Printf("Active Servers: %v\n", set.ActiveServerList)
		fmt.Printf("Contact Servers: %v\n", set.ContactServerList)

		fmt.Println("Transactions:")

		for _, tx := range set.Transactions {
			fmt.Printf("    Transaction: %d -> %d, Amount: %d\n", tx.Source, tx.Destination, tx.Amount)
			clusterIDForSource := shardMapping[tx.Source]

			// Fetch the contact server using the cluster ID and set.ContactServerList
			contactServerForSource, err := shared.GetContactServerForCluster(clusterIDForSource, set.ContactServerList)
			if err != nil {
				fmt.Printf("Error fetching contact server for cluster %s: %v\n", clusterIDForSource, err)
				continue
			}
			clusterIDForDestination := shardMapping[tx.Destination]
			// Fetch the contact server using the cluster ID and set.ContactServerList
			contactServerForDestination, err := shared.GetContactServerForCluster(clusterIDForDestination, set.ContactServerList)
			if err != nil {
				continue
			}
			// Print the contact server
			// Filter active servers for the source and destination shards
			activeServersForSourceShard := filterActiveServers(set.ActiveServerList, clusterIDForSource)
			activeServersForDestinationShard := filterActiveServers(set.ActiveServerList, clusterIDForDestination)

			transactionID := uuid.New().String()
			convertedTx := shared.Transaction{
				TransactionID: transactionID,
				Source:        tx.Source,
				Destination:   tx.Destination,
				Amount:        tx.Amount,
				Ballot:        shared.Ballot{Number: 1},
				ContactServer: 1,
				Status:        "",
			}
			// Decide whether to send intra-shard or cross-shard transaction
			startTime := time.Now()
			if contactServerForSource == contactServerForDestination {
				// Intra-shard transaction
				serverIndex, err := extractServerIndex(contactServerForSource)
				if err != nil {
				}
				convertedTx.ContactServer = serverIndex
				success, latency := client.SendIntraShardTransaction(contactServerForSource, convertedTx, activeServersForSourceShard)
				if success {
					totalTransactions++
					totalTime += latency
				}
			} else {
				// Cross-shard transaction
				convertedTx.Status = "P"

				sourceDone := make(chan struct {
					success bool
					latency time.Duration
				})
				destDone := make(chan struct {
					success bool
					latency time.Duration
				})

				// Each side gets its own copy. Both goroutines used to write
				// ContactServer on the same shared struct and then pass it by
				// value into their RPC, so whichever wrote last decided what
				// the other side sent.
				sourceTx := convertedTx
				if serverIndex, err := extractServerIndex(contactServerForSource); err == nil {
					sourceTx.ContactServer = serverIndex
				}
				destTx := convertedTx
				if serverIndex, err := extractServerIndex(contactServerForDestination); err == nil {
					destTx.ContactServer = serverIndex
				}

				go func() {
					success, latency := client.SendCrossShardTransaction(contactServerForSource, sourceTx, activeServersForSourceShard, "source")
					sourceDone <- struct {
						success bool
						latency time.Duration
					}{success, latency}
				}()

				go func() {
					success, latency := client.SendCrossShardTransaction(contactServerForDestination, destTx, activeServersForDestinationShard, "destination")
					destDone <- struct {
						success bool
						latency time.Duration
					}{success, latency}
				}()

				sourceResult := <-sourceDone
				destResult := <-destDone

				elapsedTime := time.Since(startTime)

				if sourceResult.success && destResult.success {
					totalTransactions++
					totalTime += elapsedTime
				}

				// Store this cross-shard transaction for later 2PC
				pendingCommits = append(pendingCommits, PendingCommit{
					sequence:      sequenceCounter,
					transaction:   sourceTx,
					sourceServers: activeServersForSourceShard,
					destServers:   activeServersForDestinationShard,
					shouldCommit:  sourceResult.success && destResult.success,
				})
				sequenceCounter++
			}
			time.Sleep(10 * time.Millisecond)
		}

		// Sort pendingCommits by sequence before processing
		sort.Slice(pendingCommits, func(i, j int) bool {
			return pendingCommits[i].sequence < pendingCommits[j].sequence
		})

		// Process 2PC commits in order
		// Process 2PC commits in order
		for _, pending := range pendingCommits {
			role := "2PCcommit"
			if !pending.shouldCommit {
				role = "2PCabort"
			}

			// Send 2PC to all servers in source cluster
			for _, server := range pending.sourceServers {
				client.Send2PCCommit(server, pending.transaction, role, "source")
			}

			// Send 2PC to all servers in destination cluster
			for _, server := range pending.destServers {
				client.Send2PCCommit(server, pending.transaction, role, "destination")
			}
		}

		// Clear pending commits for next set
		pendingCommits = []PendingCommit{}

		// Interactive user options
		for {
			fmt.Println("Select an option: ")
			fmt.Println("1 - Proceed to next set")
			fmt.Println("2 - Print balance")
			fmt.Println("3 - Print Datastore")
			fmt.Println("4 - Print Performance")
			fmt.Println("5 - Crash a server")
			fmt.Println("6 - Restore a server")
			fmt.Println("7 - Print cluster status")

			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			choice, err := strconv.Atoi(input)
			if err != nil {
				fmt.Println("Invalid input. Please enter a number from 1 to 7.")
				continue
			}

			switch choice {
			case 1:
				// break out of the menu loop, on to the next set
			case 2:
				PrintBalance(shardMapping, clusterServers)
				continue
			case 3:
				PrintDatastore(clusterServers)
				continue
			case 4:
				PrintPerformance(totalTransactions, totalTime)
				continue
			case 5:
				CrashServer(reader, servers)
				continue
			case 6:
				RestoreServer(reader, servers)
				continue
			case 7:
				PrintClusterStatus(clusterServers, servers)
				continue
			default:
				fmt.Println("Invalid choice. Please enter 1 to 7.")
				continue
			}
			break
		}
	}
}

// CrashServer takes a replica offline. Its listener closes, so peers see a
// connection refused exactly as they would from a dead machine, while its
// database survives on disk for the eventual restore.
func CrashServer(reader *bufio.Reader, servers map[string]*server.Server) {
	srv := promptForServer(reader, servers)
	if srv == nil {
		return
	}
	if !srv.IsRunning() {
		fmt.Printf("Server %s is already down.\n", srv.ID)
		return
	}
	if err := srv.Crash(); err != nil {
		fmt.Printf("Failed to crash server %s: %v\n", srv.ID, err)
		return
	}
	fmt.Printf("Server %s is down. Its cluster needs %d of %d replicas to keep committing.\n",
		srv.ID, shared.GetQuorumSize(), shared.GetServersPerCluster())
}

// RestoreServer brings a crashed replica back. It returns with whatever log it
// had when it died and catches up on the next prepare round.
func RestoreServer(reader *bufio.Reader, servers map[string]*server.Server) {
	srv := promptForServer(reader, servers)
	if srv == nil {
		return
	}
	if srv.IsRunning() {
		fmt.Printf("Server %s is already up.\n", srv.ID)
		return
	}
	if err := srv.Restore(); err != nil {
		fmt.Printf("Failed to restore server %s: %v\n", srv.ID, err)
		return
	}
	fmt.Printf("Server %s is back up and will catch up on the next round.\n", srv.ID)
}

// PrintClusterStatus shows which replicas are up, and whether each cluster
// still holds a quorum.
func PrintClusterStatus(clusterServers map[string][]string, servers map[string]*server.Server) {
	quorum := shared.GetQuorumSize()

	clusterIDs := make([]string, 0, len(clusterServers))
	for clusterID := range clusterServers {
		clusterIDs = append(clusterIDs, clusterID)
	}
	sort.Strings(clusterIDs)

	for _, clusterID := range clusterIDs {
		alive := 0
		states := []string{}
		for _, serverID := range clusterServers[clusterID] {
			srv, ok := servers[serverID]
			if ok && srv.IsRunning() {
				alive++
				states = append(states, fmt.Sprintf("%s UP", serverID))
			} else {
				states = append(states, fmt.Sprintf("%s DOWN", serverID))
			}
		}
		verdict := "quorum"
		if alive < quorum {
			verdict = "NO QUORUM"
		}
		fmt.Printf("%s: %s  [%d/%d alive, needs %d: %s]\n",
			clusterID, strings.Join(states, ", "), alive, len(clusterServers[clusterID]), quorum, verdict)
	}
}

func promptForServer(reader *bufio.Reader, servers map[string]*server.Server) *server.Server {
	fmt.Print("Enter server ID (e.g. S1): ")
	input, _ := reader.ReadString('\n')
	serverID := strings.ToUpper(strings.TrimSpace(input))

	srv, ok := servers[serverID]
	if !ok {
		fmt.Printf("Unknown server %q\n", serverID)
		return nil
	}
	return srv
}

func ConfigureClusters() (int, int) {
	var clusterCount, serversPerCluster int
	fmt.Println("Enter the number of clusters:")
	fmt.Scan(&clusterCount)
	fmt.Println("Enter the number of servers per cluster:")
	fmt.Scan(&serversPerCluster)
	return clusterCount, serversPerCluster
}
func GetShardsForCluster(shardMapping map[int]string, clusterID string) []int {
	shards := []int{}
	for shardID, assignedClusterID := range shardMapping {
		if assignedClusterID == clusterID {
			shards = append(shards, shardID)
		}
	}
	return shards
}

func filterActiveServers(activeServers []string, clusterID string) []string {
	var filteredServers []string
	for _, server := range activeServers {
		// Use the shared mapping to find the cluster of the server
		serverClusterID, err := shared.GetClusterID(server)
		if err == nil && serverClusterID == clusterID {
			filteredServers = append(filteredServers, server)
		} else if err != nil {
			fmt.Printf("Error fetching cluster ID for server %s: %v\n", server, err)
		}
	}
	return filteredServers
}

func InitializeClusters(clusterCount int, serversPerCluster int) map[string][]string {
	clusterServers := make(map[string][]string)

	for i := 1; i <= clusterCount; i++ {
		clusterID := fmt.Sprintf("C%d", i)
		for j := 1; j <= serversPerCluster; j++ {
			serverID := fmt.Sprintf("S%d", (i-1)*serversPerCluster+j)
			clusterServers[clusterID] = append(clusterServers[clusterID], serverID)
		}
	}

	return clusterServers
}

// AssignShardsToClusters splits dataCount accounts into contiguous ranges, one
// per cluster.
//
// The previous version used a flat dataCount/clusterCount stride, so any count
// that did not divide evenly pushed the trailing accounts into a cluster index
// past the end: with 3000 accounts and 7 clusters the last 6 accounts mapped to
// "C8", which does not exist. The remainder is now spread over the first few
// clusters instead.
func AssignShardsToClusters(dataCount int, clusterCount int) map[int]string {
	shardMapping := make(map[int]string)
	if clusterCount <= 0 || dataCount <= 0 {
		return shardMapping
	}

	base := dataCount / clusterCount
	remainder := dataCount % clusterCount

	account := 1
	for cluster := 1; cluster <= clusterCount; cluster++ {
		size := base
		if cluster <= remainder {
			size++
		}
		clusterID := fmt.Sprintf("C%d", cluster)
		for i := 0; i < size; i++ {
			shardMapping[account] = clusterID
			account++
		}
	}

	return shardMapping
}

// main.go

func PrintBalance(shardMapping map[int]string, clusterServers map[string][]string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter client ID to get balance: ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	clientID, err := strconv.Atoi(input)
	if err != nil {
		fmt.Println("Invalid client ID")
		return
	}
	clusterID, ok := shardMapping[clientID]
	if !ok {
		fmt.Printf("Client ID %d not found in shard mapping\n", clientID)
		return
	}
	fmt.Printf("Server | %d\n", clientID)
	servers := clusterServers[clusterID]
	for _, serverID := range servers {
		serverAddress, err := shared.ServerAddresses(serverID)
		if err != nil {
			fmt.Printf("Error getting server address for %s: %v\n", serverID, err)
			continue
		}
		client, err := rpc.Dial("tcp", serverAddress)
		if err != nil {
			fmt.Printf("Error connecting to server %s: %v\n", serverID, err)
			continue
		}
		defer client.Close()
		var balance int
		err = client.Call(fmt.Sprintf("Server.%s.GetBalance", serverID), clientID, &balance)
		if err != nil {
			fmt.Printf("Error getting balance from server %s: %v\n", serverID, err)
			continue
		}
		fmt.Printf("%s     | %d\n", serverID, balance)
	}
}

// main.go

func PrintDatastore(clusterServers map[string][]string) {
	for _, servers := range clusterServers {
		for _, serverID := range servers {
			serverAddress, err := shared.ServerAddresses(serverID)
			if err != nil {
				fmt.Printf("Error getting server address for %s: %v\n", serverID, err)
				continue
			}
			client, err := rpc.Dial("tcp", serverAddress)
			if err != nil {
				fmt.Printf("Error connecting to server %s: %v\n", serverID, err)
				continue
			}
			defer client.Close()
			var transactions []shared.Transaction
			err = client.Call(fmt.Sprintf("Server.%s.CommittedTransactionsInDB", serverID), struct{}{}, &transactions)
			if err != nil {
				fmt.Printf("Error getting transactions from server %s: %v\n", serverID, err)
				continue
			}
			fmt.Printf("%s :", serverID)
			for _, tx := range transactions {
				if tx.Status == "" {
					fmt.Printf(" -> |%s,(%d,%d,%d)|", tx.Ballot, tx.Source, tx.Destination, tx.Amount)
				} else {
					fmt.Printf(" -> |%s, %s,(%d,%d,%d)|", tx.Ballot, tx.Status, tx.Source, tx.Destination, tx.Amount)
				}
			}
			fmt.Print("\n")
			fmt.Print("\n")
		}
	}
}
func PrintPerformance(totalTransactions int, totalTime time.Duration) {
	if totalTransactions == 0 {
		fmt.Println("No transactions processed.")
		return
	}
	avgLatency := totalTime / time.Duration(totalTransactions)
	throughput := float64(totalTransactions) / totalTime.Seconds()
	fmt.Printf("Total Transactions: %d\n", totalTransactions)
	fmt.Printf("Total Time: %v\n", totalTime)
	fmt.Printf("Average Latency per Transaction: %v\n", avgLatency)
	fmt.Printf("Throughput: %.2f transactions per second\n", throughput)
}

// extractServerIndex extracts the numeric index from a server ID string
func extractServerIndex(serverID string) (int, error) {
	if !strings.HasPrefix(serverID, "S") {
		return 0, fmt.Errorf("invalid server ID format: %s", serverID)
	}
	return strconv.Atoi(serverID[1:]) // Convert "S1" -> 1
}

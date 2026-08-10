Lab 3 - Distributed Systems
# Distributed Banking System

A distributed banking system implementing **Paxos consensus protocol** and **Two-Phase Commit (2PC)** for transaction management across multiple clusters and servers. This system ensures consistency, fault tolerance, and atomicity in a distributed environment.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Core Components](#core-components)
- [How It Works](#how-it-works)
- [Transaction Types](#transaction-types)
- [Consensus Protocols](#consensus-protocols)
- [Database Schema](#database-schema)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Performance Metrics](#performance-metrics)

## Overview

This distributed banking system simulates a multi-cluster, multi-server banking environment where:

- **Data is sharded** across multiple clusters
- Each cluster contains **multiple replica servers** for fault tolerance
- Transactions can be **intra-shard** (within the same cluster) or **cross-shard** (across different clusters)
- **Paxos protocol** ensures consensus within clusters
- **Two-Phase Commit (2PC)** ensures atomicity for cross-shard transactions
- The system maintains **ACID properties** in a distributed setting

## Architecture

### System Design

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Application                        │
│                    (main.go)                                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
   ┌────▼────┐                  ┌────▼────┐
   │ Cluster │                  │ Cluster │
   │    C1   │                  │    C2   │
   └────┬────┘                  └────┬────┘
        │                             │
   ┌────┴────┐                   ┌────┴────┐
   │ Server  │                   │ Server  │
   │   S1    │                   │   S4    │
   │ Server  │                   │ Server  │
   │   S2    │                   │   S5    │
   │ Server  │                   │ Server  │
   │   S3    │                   │   S6    │
   └─────────┘                   └─────────┘
```

### Key Concepts

- **Clusters**: Groups of servers that replicate the same data shards
- **Shards**: Data partitions (client accounts) distributed across clusters
- **Quorum**: Minimum number of servers needed for consensus (majority: `(N+1)/2`)
- **Leader**: Server that initiates Paxos phases
- **Contact Server**: Designated server in each cluster for handling transactions

## Key Features

1. **Distributed Consensus**
   - Paxos protocol implementation for cluster-level consensus
   - Quorum-based voting mechanism
   - Leader election through ballot numbers

2. **Cross-Shard Transaction Support**
   - Two-Phase Commit (2PC) protocol
   - Atomic commit/abort across multiple clusters
   - Transaction status tracking (Pending "P", Committed "C")

3. **Fault Tolerance**
   - Replication across multiple servers per cluster
   - Automatic recovery from longest transaction history
   - Graceful handling of server failures

4. **Data Consistency**
   - Client locking mechanism to prevent concurrent modifications
   - Balance validation before transaction execution
   - Transaction history synchronization

5. **Performance Monitoring**
   - Transaction latency tracking
   - Throughput calculation
   - Performance metrics reporting

## Technologies Used

- **Go 1.23.2**: Primary programming language
- **SQLite3**: Embedded database for persistent storage
- **RPC (net/rpc)**: Inter-server communication
- **UUID**: Unique transaction ID generation
- **CSV Parser**: Test case input processing

## Project Structure

```
distributed-banking/
├── main.go                    # Main application entry point
├── go.mod                     # Go module dependencies
├── go.sum                     # Dependency checksums
│
├── client/                    # Client-side RPC communication
│   └── client.go             # Transaction sending functions
│
├── server/                    # Server implementation
│   └── server.go             # Server RPC handlers and logic
│
├── database/                  # Database layer
│   ├── database.go           # Database initialization
│   └── util.go               # Database utility functions
│
├── paxos/                     # Paxos consensus protocol
│   └── paxos.go              # Prepare, Accept, Commit phases
│
├── csv_parser/                # CSV test case parser
│   └── csv_parser.go         # Parse transaction sets from CSV
│
├── shared/                    # Shared types and utilities
│   ├── types.go              # Common data structures
│   ├── transaction.go        # Transaction type definition
│   ├── shared.go             # Quorum calculation
│   ├── serveraddress.go      # Server address mapping
│   ├── servertoclusteridmap.go # Server-to-cluster mapping
│   ├── contactserverforclusterid.go # Contact server selection
│   ├── preparerequest.go     # Paxos prepare request type
│   ├── filteractiveservers.go # Server filtering utilities
│   ├── caluculatenewshards.go # Shard redistribution logic
│   ├── serverstatus.go       # Server status tracking
│   └── transactionqueue.go   # Transaction queue implementation
│
└── test_data/                 # Test case files
    ├── guide.csv
    ├── lab1_Test.csv
    └── New_Test_Cases_-_Lab3.csv
```

## Core Components

### 1. Main Application (`main.go`)

The orchestrator that:
- Configures cluster and server topology
- Assigns data shards to clusters
- Parses CSV test cases
- Routes transactions (intra-shard vs cross-shard)
- Manages 2PC commit/abort sequences
- Provides interactive commands for system inspection

**Key Functions:**
- `ConfigureClusters()`: User input for cluster configuration
- `InitializeClusters()`: Creates server-to-cluster mapping
- `AssignShardsToClusters()`: Distributes data shards
- `PrintBalance()`: Query client balance across replicas
- `PrintDatastore()`: Display all committed transactions
- `PrintPerformance()`: Show latency and throughput metrics

### 2. Server (`server/server.go`)

Handles all server-side operations:

**RPC Methods:**
- `HandleTransaction()`: Processes intra-shard transactions
- `HandleCrossShardTransaction()`: Processes cross-shard transactions
- `Handle2PCCommit()`: Handles 2PC commit/abort messages
- `Prepare()`: Paxos prepare phase handler
- `AcceptTransactions()`: Paxos accept phase handler
- `CommitTransactions()`: Paxos commit phase handler
- `GetBalance()`: Retrieves client balance
- `CommittedTransactionsInDB()`: Returns transaction history
- `FetchBallotNumber()`: Returns current ballot number

**Key Features:**
- Thread-safe operations using mutex locks
- Transaction history synchronization
- Client locking mechanism
- Balance validation
- Automatic recovery from longest history

### 3. Client (`client/client.go`)

Client-side RPC communication:

**Functions:**
- `ConnectToServer()`: Establishes RPC connection
- `SendIntraShardTransaction()`: Sends intra-shard transaction
- `SendCrossShardTransaction()`: Sends cross-shard transaction
- `Send2PCCommit()`: Sends 2PC commit/abort message

### 4. Paxos Protocol (`paxos/paxos.go`)

Implements the three-phase Paxos consensus:

**Phases:**
1. **FetchLongestTransactionHistory()**: Discovers the longest committed transaction history from active servers
2. **PreparePhase()**: Leader sends prepare requests with longest history
3. **AcceptPhase()**: Leader sends transaction for acceptance
4. **CommitPhase()**: Leader commits transaction to all replicas

### 5. Database Layer (`database/`)

SQLite-based persistence:

**Tables:**
- `clients`: Client balances and locks
- `transactions`: Transaction history with status

**Operations:**
- Balance management (get, update)
- Lock management (set, unset, check)
- Transaction CRUD operations
- Transaction history retrieval

### 6. CSV Parser (`csv_parser/csv_parser.go`)

Parses test case files with format:
```
SetNumber, Transaction, ActiveServers, ContactServers
1, (1,2,10), [S1,S2,S3], [S1,S4]
```

**Output:**
- Array of transaction sets
- Each set contains transactions, active servers, and contact servers

## How It Works

### System Initialization

1. **Configuration**: User specifies number of clusters and servers per cluster
2. **Cluster Setup**: Servers are assigned to clusters (e.g., S1-S3 → C1, S4-S6 → C2)
3. **Shard Assignment**: Data shards (client IDs) are distributed across clusters
4. **Server Startup**: Each server starts RPC listener on port `5000 + serverNumber`
5. **Database Initialization**: Each server creates SQLite database with assigned shards (initial balance: 10)

### Transaction Processing Flow

#### Intra-Shard Transaction

```
Client → Contact Server (Leader)
         ↓
   1. Fetch longest history from replicas
   2. Update local DB with missing transactions
   3. Prepare Phase (send longest history to replicas)
   4. Check quorum
   5. Lock clients & validate balance
   6. Accept Phase (send transaction to replicas)
   7. Commit locally
   8. Commit Phase (notify replicas)
```

#### Cross-Shard Transaction

```
Client → Source Contact Server (async) ──┐
Client → Dest Contact Server (async)   ──┼─→ Both complete
                                          │
                                          ↓
                                   2PC Decision
                                          │
                    ┌────────────────────┴────────────────────┐
                    ↓                                          ↓
           2PC Commit/Abort                           2PC Commit/Abort
         (Source Cluster)                            (Dest Cluster)
```

**Cross-Shard Steps:**
1. Both source and destination clusters process transaction independently
2. Source: Locks sender, validates balance, deducts amount
3. Destination: Locks receiver, adds amount
4. Both mark transaction as "P" (Pending)
5. After both succeed, 2PC coordinator sends commit/abort
6. On commit: Unlock clients, mark as "C" (Committed)
7. On abort: Rollback balances, unlock clients

### Paxos Consensus Protocol

**Purpose**: Ensure all servers in a cluster agree on transaction order

**Process:**
1. **Leader Election**: Server with highest ballot number becomes leader
2. **Prepare Phase**: 
   - Leader fetches longest history from all replicas
   - Sends prepare request with longest history and new ballot number
   - Replicas update their history and ballot number
3. **Accept Phase**:
   - Leader sends transaction to all replicas
   - Replicas lock clients and validate
4. **Commit Phase**:
   - Leader commits locally
   - Notifies all replicas to commit

**Quorum Requirement**: `(serversPerCluster + 1) / 2` servers must respond

### Two-Phase Commit (2PC)

**Purpose**: Ensure atomicity for cross-shard transactions

**Phases:**
1. **Voting Phase** (implicit):
   - Source and destination clusters process transaction
   - Both must succeed for commit
2. **Decision Phase**:
   - Coordinator sends commit/abort to all participants
   - Participants apply decision and release locks

**Transaction States:**
- `""` (empty): Committed intra-shard transaction
- `"P"`: Pending cross-shard transaction
- `"C"`: Committed cross-shard transaction

## Transaction Types

### 1. Intra-Shard Transaction
- Source and destination clients are in the same cluster
- Processed by single contact server
- Committed immediately using Paxos
- Status: `""` (empty string)

### 2. Cross-Shard Transaction
- Source and destination clients are in different clusters
- Processed asynchronously by both clusters
- Requires 2PC for final commit/abort
- Status: `"P"` (pending) → `"C"` (committed) or aborted

## Consensus Protocols

### Paxos Protocol

**Use Case**: Consensus within a cluster for intra-shard transactions

**Properties:**
- **Safety**: All servers agree on same transaction order
- **Liveness**: System makes progress despite failures (with quorum)
- **Fault Tolerance**: Works with up to `(N-1)/2` server failures

**Implementation Details:**
- Ballot numbers ensure leader uniqueness
- Longest history ensures no transaction loss
- Quorum ensures majority agreement

### Two-Phase Commit (2PC)

**Use Case**: Atomicity for cross-shard transactions

**Properties:**
- **Atomicity**: All-or-nothing execution
- **Consistency**: All clusters reach same decision
- **Blocking**: Can block if coordinator fails (simplified implementation)

**Implementation Details:**
- Coordinator waits for both clusters to complete
- Commit decision based on both clusters' success
- Rollback on abort restores original balances

## Database Schema

### `clients` Table

| Column     | Type    | Description                    |
|------------|---------|--------------------------------|
| client_id  | INTEGER | Primary key, client identifier |
| balance    | INTEGER | Current account balance        |
| lock       | BOOLEAN | Lock status (0=unlocked, 1=locked) |

### `transactions` Table

| Column          | Type    | Description                          |
|-----------------|---------|--------------------------------------|
| transaction_id  | TEXT    | Primary key, UUID                    |
| source          | INTEGER | Source client ID                     |
| destination     | INTEGER | Destination client ID                |
| amount          | INTEGER | Transaction amount                  |
| ballot_number   | INTEGER | Paxos ballot number                  |
| contact_server  | INTEGER | Contact server index                 |
| status          | TEXT    | Transaction status ("", "P", "C")    |
| created_at      | DATETIME| Transaction timestamp                |

## Setup Instructions

### Prerequisites

- Go 1.23.2 or later
- SQLite3 (usually included with Go SQLite driver)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd 2pc-saicharanjakkula/distributed-banking
   ```

2. **Install dependencies**:
   ```bash
   go mod download
   ```

3. **Verify installation**:
   ```bash
   go build
   ```

### Running the System

1. **Start the application**:
   ```bash
   go run main.go
   ```

2. **Configure the system**:
   - Enter number of clusters (e.g., `2`)
   - Enter number of servers per cluster (e.g., `3`)

3. **Process transactions**:
   - System reads transactions from `New_Test_Cases_-_Lab3.csv`
   - Transactions are processed set by set
   - After each set, interactive menu appears

## Usage

### Interactive Commands

After processing each transaction set, you can:

1. **Proceed to next set**: Continue to next transaction set
2. **Print balance**: Query balance for a specific client ID
   - Shows balance from all servers in the client's cluster
   - Useful for verifying consistency across replicas
3. **Print Datastore**: Display all committed transactions
   - Shows transaction history from all servers
   - Format: `|<ballot,contact>,status,(source,dest,amount)|`
4. **Print Performance**: Display performance metrics
   - Total transactions processed
   - Total time elapsed
   - Average latency per transaction
   - Throughput (transactions per second)

### Example Session

```
Enter the number of clusters: 2
Enter the number of servers per cluster: 3

Clusters initialized: map[C1:[S1 S2 S3] C2:[S4 S5 S6]]
Processing Set 1
Active Servers: [S1 S2 S3 S4 S5 S6]
Contact Servers: [S1 S4]

Transactions:
    Transaction: 1 -> 2, Amount: 10
    Transaction: 5 -> 6, Amount: 5

Select an option:
1 - Proceed to next set
2 - Print balance
3 - Print Datastore
4 - Print Performance
```

### CSV Test Case Format

The CSV file should have the following format:

```csv
SetNumber, Transaction, ActiveServers, ContactServers
1, (1,2,10), [S1,S2,S3,S4], [S1,S4]
, (3,4,5), [S1,S2,S3,S4], [S1,S4]
2, (5,6,15), [S1,S2,S5,S6], [S1,S5]
```

- **SetNumber**: Transaction set identifier (first row of each set)
- **Transaction**: `(source,destination,amount)`
- **ActiveServers**: List of active servers for this set `[S1,S2,...]`
- **ContactServers**: List of contact servers per cluster `[S1,S4,...]`

## Performance Metrics

The system tracks and reports:

- **Total Transactions**: Number of successfully processed transactions
- **Total Time**: Cumulative time for all transactions
- **Average Latency**: `Total Time / Total Transactions`
- **Throughput**: `Total Transactions / Total Time (seconds)`

### Performance Considerations

- **Intra-shard transactions**: Faster (single cluster consensus)
- **Cross-shard transactions**: Slower (requires 2PC coordination)
- **Quorum size**: Affects fault tolerance vs. performance trade-off
- **Network latency**: RPC calls between servers add overhead
- **Lock contention**: Concurrent transactions on same clients may conflict

## Key Design Decisions

1. **SQLite for Persistence**: Lightweight, embedded database suitable for simulation
2. **RPC for Communication**: Simple, synchronous communication model
3. **Mutex-based Locking**: Ensures thread safety on each server
4. **Longest History Recovery**: Prevents transaction loss during failures
5. **Sequential 2PC Processing**: Maintains transaction order for cross-shard transactions
6. **Dynamic Port Assignment**: Ports assigned as `5000 + serverNumber` for scalability

## Limitations and Future Improvements

### Current Limitations

1. **Blocking 2PC**: Coordinator failure can block the system
2. **No Network Partition Handling**: Assumes reliable network
3. **Sequential Cross-Shard Processing**: Could be optimized for parallelism
4. **No Leader Election Protocol**: Uses highest ballot number implicitly
5. **Simplified Failure Model**: Assumes servers either work or fail completely

### Potential Improvements

1. **Three-Phase Commit (3PC)**: Non-blocking commit protocol
2. **Raft Consensus**: Alternative consensus algorithm with explicit leader election
3. **Parallel 2PC**: Process multiple cross-shard transactions concurrently
4. **Network Partition Tolerance**: Handle split-brain scenarios
5. **Checkpointing**: Periodic state snapshots for faster recovery
6. **Load Balancing**: Distribute transactions across multiple contact servers
7. **Monitoring Dashboard**: Real-time visualization of system state

## Testing

The system includes test case files:
- `guide.csv`: Example transactions
- `lab1_Test.csv`: Lab 1 test cases
- `New_Test_Cases_-_Lab3.csv`: Lab 3 test cases

To use a different test file, modify the filename in `main.go`:
```go
sets, err := csv_parser.ParseCSV("your_test_file.csv")
```

## Troubleshooting

### Common Issues

1. **Port Already in Use**:
   - Ensure previous server instances are terminated
   - Check for processes using ports 5000-5999

2. **Database Lock Errors**:
   - Close all database connections before restarting
   - Delete `db_*.db` files if corrupted

3. **Transaction Failures**:
   - Check quorum size (need majority of active servers)
   - Verify client balances are sufficient
   - Ensure clients are not locked by other transactions

4. **RPC Connection Errors**:
   - Verify servers are running
   - Check server addresses in `shared/serveraddress.go`
   - Ensure network connectivity

## License

[Specify your license here]

## Authors

- Sai Charan Jakkula

## Acknowledgments

This project implements distributed systems concepts including:
- Paxos consensus algorithm
- Two-Phase Commit protocol
- Distributed transaction processing
- Replication and fault tolerance

---

For questions or issues, please refer to the code comments or create an issue in the repository.

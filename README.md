# Distributed Banking System — Paxos + Two-Phase Commit

A sharded, replicated bank ledger in Go. Accounts are partitioned across clusters; each cluster is replicated across several servers. **Paxos** keeps the replicas within a cluster agreed on one ordered log, and **two-phase commit** makes a transfer that spans two clusters atomic across both.

It started as a graduate distributed-systems lab. This version fixes several safety bugs in the original — the interesting part of the repo is arguably

---

## Contents

- [How it works](#how-it-works)
- [Quickstart](#quickstart)
- [Running the tests](#running-the-tests)
- [Failure demos](#failure-demos)
- [Screenshots](#screenshots)
- [What changed and why](#what-changed-and-why)
- [Limitations](#limitations)
- [Project layout](#project-layout)

---

## How it works

### Topology

You pick the cluster count and replicas per cluster at startup. Accounts are split into contiguous ranges, one per cluster. The default configuration:

| Cluster | Accounts | Replicas | Quorum |
|---------|----------|----------|--------|
| C1 | 1–1000 | S1, S2, S3 | 2 of 3 |
| C2 | 1001–2000 | S4, S5, S6 | 2 of 3 |
| C3 | 2001–3000 | S7, S8, S9 | 2 of 3 |

Every account starts with a balance of 10. Server `S<n>` listens on `localhost:500<n>` and keeps its own SQLite file (`db_S<n>.db`) holding a `clients` table (balance + lock flag) and a `transactions` table (the replicated log).

Workload comes from a CSV of transaction *sets*. Each set names the transfers plus two lists: which servers are active, and which server is the contact (leader) for each cluster.

### Intra-shard transfer — Paxos only

When both accounts live in the same cluster, the contact server runs one Paxos round:

1. **Ballot.** The leader picks a ballot strictly above anything it has seen: `Ballot{Number, ServerID}`, ordered by number then server.
2. **Prepare.** Broadcast to all replicas in parallel. An acceptor promises only if the ballot is strictly greater than its current promise, and its reply carries whatever it has already accepted.
3. **Count.** The leader proceeds only on a strict majority of promises. It adopts the value accepted under the highest ballot in that quorum — its own log is not automatically the winner.
4. **Local check.** Sender unlocked and solvent → lock both accounts.
5. **Accept.** Broadcast the proposal stamped with the ballot. Acceptors reject anything below their promise.
6. **Count again.** Only a majority of accepts makes the value *chosen*. Short of that, the leader releases its locks and gives up.
7. **Commit.** Apply locally, then broadcast so the rest of the cluster applies it too.

### Cross-shard transfer — Paxos under 2PC

When the accounts live in different clusters, each side runs its own Paxos round in parallel:

- **Source cluster** — reach quorum, verify funds, lock the sender, debit it, write the entry with status `P` (prepared), and **keep the lock held**.
- **Destination cluster** — reach quorum, lock the receiver, credit it, write status `P`, **lock held**.

Each side reaching quorum is that shard's **yes vote**. A shard that loses quorum, finds the account locked, or finds insufficient funds votes no.

The coordinator then applies the 2PC rule — unanimity or abort — and broadcasts the decision to every replica in both clusters:

- **`2PCcommit`** → flip the entry to `C`, release locks. The money has moved.
- **`2PCabort`** → reverse the balance change, release locks.

---

## Quickstart

Requires Go 1.25+. No C toolchain: the SQLite driver is pure Go, so everything builds with `CGO_ENABLED=0`.

```bash
cd distributed-banking && go run .
```

You will be prompted for the number of clusters and servers per cluster (3 and 3 reproduces the table above). The system then walks through the CSV one set at a time, pausing at a menu after each:

```
1 - Proceed to next set
2 - Print balance
3 - Print Datastore
4 - Print Performance
5 - Crash a server
6 - Restore a server
7 - Print cluster status
```

**Print Datastore** is the one to look at: it prints each replica's log as a chain of `<ballot,server>,(source,destination,amount)` entries, so you can see directly whether the replicas converged.

---

## Running the tests

```bash
cd distributed-banking && go test -race ./... -count=1
```

The suite spins up a real cluster in-process — real listeners, real RPCs — and asserts the guarantees that actually matter:

| Test | What it pins down |
|------|-------------------|
| `TestCommitWithFullClusterReplicatesToAll` | A healthy cluster commits and all replicas match |
| `TestCommitProceedsWithMinorityDown` | One replica down out of three still makes progress |
| `TestRefusesToCommitWithoutQuorum` | Two down out of three → the survivor refuses rather than committing alone |
| `TestFailedRoundReleasesLocks` | A failed round leaves no account stuck locked |
| `TestNoMoneyCreatedOrDestroyed` | Balances sum to the same total after a run of transfers |
| `TestReplicasConvergeOnIdenticalLogs` | Every replica holds the same entries in the same order, under the same ballots |
| `TestRecoveredReplicaCatchesUp` | A restarted replica catches up instead of staying behind |
| `TestReconciliationNeverDropsCommittedEntries` | Syncing from a peer never deletes committed state |
| `TestBallotOrdering`, `TestSameRoundDifferentServersAreOrdered` | Ballots form a strict total order even when two servers reach the same round |
| `TestQuorumSizeIsStrictMajority` | No two disjoint quorums are possible at any cluster size |
| `TestAssignShardsToClustersCoversEveryAccount` | Every account maps to a cluster that exists, for any cluster count |
| `TestMigrateAddsBallotServerToLegacySchema` | Databases from the old schema upgrade in place without losing rows |

---

## Failure demos

Menu option **5** crashes a replica by closing its listener — peers get a connection refused exactly as they would from a dead machine, while its database survives on disk. Option **6** brings it back; it returns with the log it died with and catches up on the next prepare round. Option **7** prints which replicas are up and whether each cluster still holds a quorum.

The sequence worth demonstrating:

1. Run a set with all replicas up → commits, all logs match.
2. Crash one replica in a cluster (2 of 3 alive) → still commits, quorum holds.
3. Crash a second (1 of 3 alive) → the leader **refuses**; balances unchanged.
4. Restore both, run another transfer → the recovered replicas catch up.

---

## Screenshots

Captures live in [`docs/screenshots/`](docs/screenshots/). See that directory's README for the exact capture checklist.

| | |
|---|---|
| **Cluster startup** — 3×3 topology initialising | ![Cluster startup](docs/screenshots/01-cluster-startup.png) |
| **Intra-shard commit** — one Paxos round | ![Intra-shard commit](docs/screenshots/02-intra-shard-commit.png) |
| **Cross-shard commit** — two rounds under 2PC | ![Cross-shard commit](docs/screenshots/03-cross-shard-commit.png) |
| **Cross-shard abort** — insufficient funds | ![Cross-shard abort](docs/screenshots/04-cross-shard-abort.png) |
| **Minority down** — consensus still reached | ![Minority down](docs/screenshots/05-minority-down-commits.png) |
| **Majority down** — leader refuses | ![Majority down](docs/screenshots/06-majority-down-refuses.png) |
| **Recovery** — restarted replica catches up | ![Recovery](docs/screenshots/07-recovery-catch-up.png) |
| **Datastore** — identical logs across replicas | ![Datastore](docs/screenshots/08-datastore-converged.png) |
| **Performance** — throughput and latency | ![Performance](docs/screenshots/09-performance.png) |
| **Test suite** — green under `-race` | ![Tests](docs/screenshots/10-tests-green.png) |

---

## What changed and why

The original passed the course's test cases but violated Paxos safety in several places. Each fix below has a test that fails without it.

### Ballots had no total order

Ballots were a bare `int`. Two servers in a cluster can independently reach the same counter value, and an acceptor comparing only counters cannot tell those proposals apart — so it followed both.

Ballots are now `Ballot{Number, ServerID}`, ordered by number then proposing server. Two leaders at round 1 produce `<1,1>` and `<1,2>`, which are strictly ordered, so acceptors can always tell which proposal is newer.

### Acceptors never rejected anything

`Prepare` did `s.BallotNumber = request.LeaderBallotNumber` unconditionally — it adopted whatever ballot arrived, letting a stale leader reclaim a cluster that had already promised to a newer one.

Acceptors now promise only to a strictly greater ballot, reject accepts below their promise, and return what they have already accepted so the leader can adopt an in-flight value rather than overwrite it.

### The leader never counted votes

`PreparePhase` and `AcceptPhase` looped over peers, fired an RPC, and **discarded both the reply and the error**. Nothing was ever tallied, so the leader committed regardless of what the cluster said. This is the bug that meant the code did not actually implement Paxos.

Both phases now broadcast concurrently, collect replies, and return a count. The leader commits only on a strict majority at *both* phases, and releases its locks when a round fails. Every RPC has a timeout, so a dead peer fails fast instead of hanging the round.

### The quorum check consulted the test fixture

The old gate was `len(request.ActiveServers) < GetQuorumSize()` — how many servers the **CSV file claimed** were up, not how many replied. A partitioned leader whose CSV still listed nine servers would commit alone.

That check is gone. The gate is now the promise and accept counts returned by the phases themselves.

### Quorum size was not a majority

`GetQuorumSize` returned `(n+1)/2`, which is a majority only for odd `n`. With four servers it returned 2, so two disjoint halves could each believe they held a quorum. Now `n/2 + 1`.

### Reconciliation destroyed committed state

`FetchLongestTransactionHistory` picked whichever replica reported the most rows, then `DELETE`d every local row and rewrote the table from that peer. Length is not authority, and destroying durable history to synchronise means one bad peer can erase a replica's log.

Reconciliation is now additive: entries already present are left alone, missing entries are inserted, and nothing is ever deleted.

### A data race decided what got sent

Both cross-shard goroutines wrote `ContactServer` on the same shared transaction struct before passing it by value into their RPC, so whichever wrote last silently decided what the other side sent. Each side now gets its own copy. The suite runs clean under `-race`.

### Accounts could be locked forever

A round that took locks and then failed left both accounts locked with no path to release, so every later transfer touching them aborted. Failed rounds now release what they took.

### Shard assignment could map past the end

`AssignShardsToClusters` used a flat `dataCount/clusterCount` stride, so any count that did not divide evenly pushed trailing accounts into a cluster index past the end — 3000 accounts over 7 clusters put the last 6 in `C8`, which does not exist. The remainder is now spread over the leading clusters.

### Housekeeping

- Swapped `mattn/go-sqlite3` (cgo) for `modernc.org/sqlite` (pure Go), so the project builds with no C toolchain.
- `GetAllTransactions` returned `[]map[string]interface{}` and callers did unchecked `.(int)` assertions that would panic on any surprise. It returns `[]shared.Transaction` now, deleting those sites entirely.
- Connections were `defer client.Close()`d **inside loops**, leaking until the whole phase returned. They close per iteration.
- Removed ~145 lines of commented-out debug printing and three unused files.
- Replaced the CI workflow — which POSTed the repo and commit SHA to a course grading endpoint — with build, vet, gofmt, and `go test -race`.

---

## Limitations

Stated plainly, because they are real:

- **The replicas are goroutines in one process.** They communicate over genuine TCP RPC with real listeners, and crashing one produces a real connection refused — but they are not separate OS processes. Running them as separate processes would need a config file and a `cmd/server` split.
- **The 2PC coordinator is not crash-recoverable.** It holds its decision in memory with no write-ahead log. If it dies between the votes and the decision broadcast, participants are left holding locks with no one to resolve them. A durable decision log plus a recovery pass on restart is the fix.
- **No leader election.** The contact server for each cluster comes from the CSV rather than being elected. Paxos will correctly reject a stale leader, but nothing promotes a new one automatically.
- **Locks are held across network calls.** `HandleTransaction` holds the server mutex for its whole body, including synchronous RPCs to peers that take their own locks. The 10 ms gap between transactions is currently what keeps this from deadlocking.
- **The log has no explicit index.** Entries are ordered by insertion time rather than by a monotonic position, which is enough here but would not survive concurrent leaders writing at the same index.

---

## Project layout

```
distributed-banking/
├── main.go              Workload driver, 2PC coordinator, operator menu
├── main_test.go         Shard assignment tests
├── client/              RPC client helpers
├── csv_parser/          Test-case CSV reader
├── database/            SQLite schema, migrations, queries
├── paxos/               Prepare / Accept / Commit phases, quorum counting
├── server/              Replica: acceptor logic, leader logic, lifecycle
│   └── cluster_test.go  In-process cluster integration tests
└── shared/              Ballot, Transaction, addressing, quorum math
```

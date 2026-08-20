package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"distributed-banking/database"
	"distributed-banking/shared"

	"github.com/google/uuid"
)

// startCluster brings up a single cluster of n replicas in a temp directory and
// returns them keyed by server ID. Each test gets its own on-disk state.
func startCluster(t *testing.T, n int, accounts []int) map[string]*Server {
	t.Helper()

	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(wd) })

	shared.SetServersPerCluster(n)

	servers := make(map[string]*Server, n)
	ids := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("S%d", i))
	}
	shared.InitializeServerToClusterMapping(map[string][]string{"C1": ids})

	for _, id := range ids {
		addr, err := shared.ServerAddresses(id)
		if err != nil {
			t.Fatal(err)
		}
		srv := StartServerRPC(id, addr, "C1", accounts)
		if srv == nil {
			t.Fatalf("failed to start %s", id)
		}
		servers[id] = srv
	}

	t.Cleanup(func() {
		for _, srv := range servers {
			srv.Stop()
		}
		// Give the OS a moment to release the ports before the next test binds.
		time.Sleep(50 * time.Millisecond)
	})

	return servers
}

func serverIDs(servers map[string]*Server) []string {
	ids := make([]string, 0, len(servers))
	for id := range servers {
		ids = append(ids, id)
	}
	return ids
}

func transfer(source, destination, amount int) shared.Transaction {
	return shared.Transaction{
		TransactionID: uuid.New().String(),
		Source:        source,
		Destination:   destination,
		Amount:        amount,
		ContactServer: 1,
	}
}

func balance(t *testing.T, srv *Server, account int) int {
	t.Helper()
	b, err := database.GetClientBalance(srv.Database.DB, account)
	if err != nil {
		t.Fatalf("balance for %d on %s: %v", account, srv.ID, err)
	}
	return b
}

func logOf(t *testing.T, srv *Server) []shared.Transaction {
	t.Helper()
	txs, err := database.GetAllTransactions(srv.Database.DB)
	if err != nil {
		t.Fatalf("log for %s: %v", srv.ID, err)
	}
	return txs
}

const committed = "Transaction successfully handled by the leader."

// A healthy cluster commits, and every replica ends up with the same log.
func TestCommitWithFullClusterReplicatesToAll(t *testing.T) {
	accounts := []int{1, 2}
	servers := startCluster(t, 3, accounts)
	leader := servers["S1"]

	var reply string
	tx := transfer(1, 2, 4)
	if err := leader.HandleTransaction(TransactionRequest{Transaction: tx, ActiveServers: serverIDs(servers)}, &reply); err != nil {
		t.Fatalf("HandleTransaction: %v", err)
	}
	if reply != committed {
		t.Fatalf("leader did not commit: %q", reply)
	}

	for id, srv := range servers {
		if got := balance(t, srv, 1); got != 6 {
			t.Errorf("%s: account 1 = %d, want 6", id, got)
		}
		if got := balance(t, srv, 2); got != 14 {
			t.Errorf("%s: account 2 = %d, want 14", id, got)
		}
		if got := len(logOf(t, srv)); got != 1 {
			t.Errorf("%s: log has %d entries, want 1", id, got)
		}
	}
}

// One replica down out of three still leaves a majority, so the cluster must
// keep making progress.
func TestCommitProceedsWithMinorityDown(t *testing.T) {
	accounts := []int{1, 2}
	servers := startCluster(t, 3, accounts)

	if err := servers["S3"].Crash(); err != nil {
		t.Fatal(err)
	}

	var reply string
	tx := transfer(1, 2, 3)
	if err := servers["S1"].HandleTransaction(TransactionRequest{Transaction: tx, ActiveServers: serverIDs(servers)}, &reply); err != nil {
		t.Fatalf("HandleTransaction: %v", err)
	}
	if reply != committed {
		t.Fatalf("cluster stalled with only one replica down: %q", reply)
	}

	for _, id := range []string{"S1", "S2"} {
		if got := balance(t, servers[id], 1); got != 7 {
			t.Errorf("%s: account 1 = %d, want 7", id, got)
		}
	}
}

// Two replicas down out of three leaves no majority. The survivor must refuse
// rather than commit alone. This is the split-brain case the old code allowed,
// because it counted the servers the caller CLAIMED were up rather than the
// ones that actually replied.
func TestRefusesToCommitWithoutQuorum(t *testing.T) {
	accounts := []int{1, 2}
	servers := startCluster(t, 3, accounts)

	if err := servers["S2"].Crash(); err != nil {
		t.Fatal(err)
	}
	if err := servers["S3"].Crash(); err != nil {
		t.Fatal(err)
	}

	leader := servers["S1"]
	before1, before2 := balance(t, leader, 1), balance(t, leader, 2)

	var reply string
	tx := transfer(1, 2, 5)
	// ActiveServers still lists all three: the caller believes the cluster is
	// healthy. Only the replies may be trusted.
	if err := leader.HandleTransaction(TransactionRequest{Transaction: tx, ActiveServers: serverIDs(servers)}, &reply); err != nil {
		t.Fatalf("HandleTransaction: %v", err)
	}

	// Be specific about WHY it refused, so this cannot pass for an unrelated
	// reason such as the balance check happening to fail.
	if reply != "no quorum for prepare" {
		t.Fatalf("leader reply = %q, want refusal at the prepare quorum", reply)
	}
	if got := balance(t, leader, 1); got != before1 {
		t.Errorf("account 1 changed to %d despite no quorum (was %d)", got, before1)
	}
	if got := balance(t, leader, 2); got != before2 {
		t.Errorf("account 2 changed to %d despite no quorum (was %d)", got, before2)
	}
	if got := len(logOf(t, leader)); got != 0 {
		t.Errorf("leader wrote %d log entries without a quorum", got)
	}
}

// A failed round must not leave accounts locked, or every later transaction
// touching them would abort forever.
func TestFailedRoundReleasesLocks(t *testing.T) {
	accounts := []int{1, 2}
	servers := startCluster(t, 3, accounts)
	leader := servers["S1"]

	servers["S2"].Crash()
	servers["S3"].Crash()

	var reply string
	leader.HandleTransaction(TransactionRequest{Transaction: transfer(1, 2, 5), ActiveServers: serverIDs(servers)}, &reply)

	for _, account := range accounts {
		locked, err := database.IsLocked(leader.Database.DB, account)
		if err != nil {
			t.Fatal(err)
		}
		if locked {
			t.Errorf("account %d left locked after a failed round", account)
		}
	}

	// Bring the cluster back; the same transfer should now succeed.
	if err := servers["S2"].Restore(); err != nil {
		t.Fatal(err)
	}
	if err := servers["S3"].Restore(); err != nil {
		t.Fatal(err)
	}

	if err := leader.HandleTransaction(TransactionRequest{Transaction: transfer(1, 2, 5), ActiveServers: serverIDs(servers)}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply != committed {
		t.Fatalf("recovered cluster still refuses to commit: %q", reply)
	}
}

// Money is conserved: a run of transfers may move value around but must never
// create or destroy it.
func TestNoMoneyCreatedOrDestroyed(t *testing.T) {
	accounts := []int{1, 2, 3, 4}
	servers := startCluster(t, 3, accounts)
	leader := servers["S1"]

	const startingBalance = 10
	total := len(accounts) * startingBalance

	transfers := []struct{ from, to, amount int }{
		{1, 2, 3}, {2, 3, 5}, {3, 4, 2}, {4, 1, 7}, {1, 3, 4},
		{2, 4, 9}, // may abort on insufficient funds; either outcome is fine
		{3, 1, 1},
	}

	for _, tr := range transfers {
		var reply string
		if err := leader.HandleTransaction(TransactionRequest{
			Transaction:   transfer(tr.from, tr.to, tr.amount),
			ActiveServers: serverIDs(servers),
		}, &reply); err != nil {
			t.Fatalf("transfer %d to %d: %v", tr.from, tr.to, err)
		}
	}

	for id, srv := range servers {
		sum := 0
		for _, account := range accounts {
			b := balance(t, srv, account)
			if b < 0 {
				t.Errorf("%s: account %d went negative (%d)", id, account, b)
			}
			sum += b
		}
		if sum != total {
			t.Errorf("%s: balances sum to %d, want %d", id, sum, total)
		}
	}
}

// Every replica must converge on the same ordered log.
func TestReplicasConvergeOnIdenticalLogs(t *testing.T) {
	accounts := []int{1, 2, 3}
	servers := startCluster(t, 3, accounts)
	leader := servers["S1"]

	for _, tr := range []struct{ from, to, amount int }{{1, 2, 2}, {2, 3, 3}, {3, 1, 1}} {
		var reply string
		if err := leader.HandleTransaction(TransactionRequest{
			Transaction:   transfer(tr.from, tr.to, tr.amount),
			ActiveServers: serverIDs(servers),
		}, &reply); err != nil {
			t.Fatal(err)
		}
	}

	want := logOf(t, leader)
	if len(want) != 3 {
		t.Fatalf("leader log has %d entries, want 3", len(want))
	}

	for id, srv := range servers {
		got := logOf(t, srv)
		if len(got) != len(want) {
			t.Fatalf("%s: log has %d entries, want %d", id, len(got), len(want))
		}
		for i := range want {
			if got[i].TransactionID != want[i].TransactionID {
				t.Errorf("%s: entry %d is %s, want %s", id, i, got[i].TransactionID, want[i].TransactionID)
			}
			if got[i].Ballot != want[i].Ballot {
				t.Errorf("%s: entry %d has ballot %v, want %v", id, i, got[i].Ballot, want[i].Ballot)
			}
		}
	}
}

// A crashed replica that comes back must catch up rather than stay behind.
func TestRecoveredReplicaCatchesUp(t *testing.T) {
	accounts := []int{1, 2}
	servers := startCluster(t, 3, accounts)
	leader := servers["S1"]

	if err := servers["S3"].Crash(); err != nil {
		t.Fatal(err)
	}

	var reply string
	for i := 0; i < 2; i++ {
		if err := leader.HandleTransaction(TransactionRequest{
			Transaction:   transfer(1, 2, 1),
			ActiveServers: serverIDs(servers),
		}, &reply); err != nil {
			t.Fatal(err)
		}
	}

	if got := len(logOf(t, servers["S3"])); got != 0 {
		t.Fatalf("crashed replica somehow logged %d entries", got)
	}

	if err := servers["S3"].Restore(); err != nil {
		t.Fatal(err)
	}

	// The next round carries the adopted log to the recovered replica.
	if err := leader.HandleTransaction(TransactionRequest{
		Transaction:   transfer(1, 2, 1),
		ActiveServers: serverIDs(servers),
	}, &reply); err != nil {
		t.Fatal(err)
	}

	if got := len(logOf(t, servers["S3"])); got == 0 {
		t.Error("recovered replica did not catch up")
	}
}

// Reconciliation must never delete committed state. The old implementation
// wiped the table whenever a peer reported a longer log.
func TestReconciliationNeverDropsCommittedEntries(t *testing.T) {
	accounts := []int{1, 2}
	servers := startCluster(t, 3, accounts)
	srv := servers["S1"]

	existing := transfer(1, 2, 2)
	existing.Ballot = shared.Ballot{Number: 5, ServerID: 1}
	existing.Status = "C"
	if err := database.AddTransaction(srv.Database.DB, existing.TransactionID, existing.Source,
		existing.Destination, existing.Amount, existing.Ballot, existing.ContactServer, existing.Status); err != nil {
		t.Fatal(err)
	}

	// A peer offers a longer log that does not contain our entry.
	incoming := []shared.Transaction{transfer(2, 1, 1), transfer(1, 2, 1), transfer(2, 1, 1)}
	for i := range incoming {
		incoming[i].Ballot = shared.Ballot{Number: 9, ServerID: 2}
		incoming[i].Status = "C"
	}

	if err := srv.UpdateCommittedTransactionsInDB(incoming); err != nil {
		t.Fatal(err)
	}

	if _, err := database.GetTransaction(srv.Database.DB, existing.TransactionID); err != nil {
		t.Fatalf("committed entry was destroyed by reconciliation: %v", err)
	}
	if got := len(logOf(t, srv)); got != 4 {
		t.Errorf("log has %d entries, want 4 (1 existing + 3 adopted)", got)
	}
}

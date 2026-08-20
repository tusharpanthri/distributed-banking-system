package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"distributed-banking/shared"
)

// A database written before ballots carried a server ID has no ballot_server
// column. Opening it must migrate it in place rather than failing or silently
// dropping the existing rows.
func TestMigrateAddsBallotServerToLegacySchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db_legacy.db")

	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`
	CREATE TABLE clients (
		client_id INTEGER PRIMARY KEY,
		balance INTEGER NOT NULL,
		lock BOOLEAN NOT NULL DEFAULT 0
	);
	CREATE TABLE transactions (
		transaction_id TEXT PRIMARY KEY,
		source INTEGER NOT NULL,
		destination INTEGER NOT NULL,
		amount INTEGER NOT NULL,
		ballot_number INTEGER NOT NULL,
		contact_server INTEGER NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	INSERT INTO transactions (transaction_id, source, destination, amount, ballot_number, contact_server, status)
	VALUES ('legacy-tx', 1, 2, 5, 4, 1, 'C');
	`)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)

	d, err := InitDatabase("legacy", []int{1, 2})
	if err != nil {
		t.Fatalf("InitDatabase on legacy schema: %v", err)
	}
	defer d.DB.Close()

	has, err := hasColumn(d.DB, "transactions", "ballot_server")
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("ballot_server column was not added")
	}

	// The pre-existing row must survive, defaulted to server 0.
	tx, err := GetTransaction(d.DB, "legacy-tx")
	if err != nil {
		t.Fatalf("legacy row lost: %v", err)
	}
	if tx.Ballot != (shared.Ballot{Number: 4, ServerID: 0}) {
		t.Fatalf("legacy ballot = %v, want <4,0>", tx.Ballot)
	}

	// Migration is idempotent.
	if err := migrate(d.DB); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestBallotRoundTrips(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)

	d, err := InitDatabase("roundtrip", []int{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	defer d.DB.Close()

	want := shared.Ballot{Number: 7, ServerID: 3}
	if err := AddTransaction(d.DB, "tx-1", 1, 2, 5, want, 1, "C"); err != nil {
		t.Fatal(err)
	}

	tx, err := GetTransaction(d.DB, "tx-1")
	if err != nil {
		t.Fatal(err)
	}
	if tx.Ballot != want {
		t.Fatalf("GetTransaction ballot = %v, want %v", tx.Ballot, want)
	}

	all, err := GetAllTransactions(d.DB)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Ballot != want {
		t.Fatalf("GetAllTransactions = %+v, want one row with ballot %v", all, want)
	}
}

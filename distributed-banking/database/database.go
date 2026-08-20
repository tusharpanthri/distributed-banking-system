package database

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type Database struct {
	DB       *sql.DB
	ServerID string
	ShardID  string
}

func InitDatabase(serverID string, shardIDs []int) (*Database, error) {
	filePath := fmt.Sprintf("db_%s.db", serverID)
	db, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database for server %s: %v", serverID, err)
	}

	// Create a single table for client/shard balances and locks
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS clients (
		client_id INTEGER PRIMARY KEY,
		balance INTEGER NOT NULL,
		lock BOOLEAN NOT NULL DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS transactions (
		transaction_id TEXT PRIMARY KEY,
		source INTEGER NOT NULL,
		destination INTEGER NOT NULL,
		amount INTEGER NOT NULL,
		ballot_number INTEGER NOT NULL,
		ballot_server INTEGER NOT NULL DEFAULT 0,
		contact_server INTEGER NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables for server %s: %v", serverID, err)
	}

	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("failed to migrate schema for server %s: %v", serverID, err)
	}

	// Initialize the clients table with an initial balance of 10 and lock set to false
	for _, shardID := range shardIDs {
		_, err = db.Exec(`
		INSERT OR IGNORE INTO clients (client_id, balance, lock)
		VALUES (?, 10, 0)`, shardID)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize client ID %d for server %s: %v", shardID, serverID, err)
		}
	}

	return &Database{DB: db, ServerID: serverID}, nil
}

func ClearTransactions(db *sql.DB) error {
	_, err := db.Exec("DELETE FROM transactions")
	if err != nil {
		return fmt.Errorf("failed to clear transactions table: %v", err)
	}
	return nil
}

// migrate brings a database created by an earlier version of the schema up to
// date. Ballots used to be a bare integer, so transactions tables written
// before the Ballot struct landed have no ballot_server column.
func migrate(db *sql.DB) error {
	has, err := hasColumn(db, "transactions", "ballot_server")
	if err != nil {
		return err
	}
	if !has {
		if _, err := db.Exec(`ALTER TABLE transactions ADD COLUMN ballot_server INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("failed to add ballot_server column: %v", err)
		}
	}
	return nil
}

func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, fmt.Errorf("failed to inspect table %s: %v", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			ctype      string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, rows.Err()
		}
	}
	return false, rows.Err()
}

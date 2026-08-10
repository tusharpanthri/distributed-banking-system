package database

import (
	"database/sql"
	"distributed-banking/shared"
	"fmt"
)

// Fetches the balance for a specific client ID
func GetClientBalance(db *sql.DB, clientID int) (int, error) {
	var balance int
	row := db.QueryRow(`SELECT balance FROM clients WHERE client_id = ?`, clientID)
	err := row.Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("client ID %d not found", clientID)
		}
		return 0, err
	}
	return balance, nil
}

// Updates the balance for a specific client ID
func UpdateClientBalance(db *sql.DB, clientID int, amount int) error {
	_, err := db.Exec(`UPDATE clients SET balance = balance + ? WHERE client_id = ?`, amount, clientID)
	if err != nil {
		return fmt.Errorf("failed to update balance for client ID %d: %v", clientID, err)
	}
	return nil
}

// Inserts a new transaction into the transactions table with a timestamp
func AddTransaction(db *sql.DB, txID string, source int, destination int, amount int, ballot_number int, contact_server int, status string) error {
	// fmt.Printf("Adding transaction: ID=%s, Source=%d, Destination=%d, Amount=%d, Status=%s\n", txID, source, destination, amount, status)
	_, err := db.Exec(`
		INSERT INTO transactions (transaction_id, source, destination, amount, ballot_number, contact_server, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		txID, source, destination, amount, ballot_number, contact_server, status,
	)
	if err != nil {
		return fmt.Errorf("failed to add transaction %s: %v", txID, err)
	}
	return nil
}

// // Updates the status of a specific transaction
// func UpdateTransactionStatus(db *sql.DB, txID string, status string) error {
// 	_, err := db.Exec(`UPDATE transactions SET status = ? WHERE transaction_id = ?`, status, txID)
// 	if err != nil {
// 		return fmt.Errorf("failed to update transaction status for %s: %v", txID, err)
// 	}
// 	return nil
// }

// // Retrieves all transactions with a specific status, ordered by creation time
// func GetTransactionsByStatus(db *sql.DB, status string) ([]map[string]interface{}, error) {
// 	rows, err := db.Query(`
// 		SELECT transaction_id, source, destination, amount, ballot_number, contact_server, status, created_at
// 		FROM transactions
// 		WHERE status = ?
// 		ORDER BY created_at ASC`, status)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to fetch transactions with status %s: %v", status, err)
// 	}
// 	defer rows.Close()

// 	transactions := make([]map[string]interface{}, 0)
// 	for rows.Next() {
// 		var txID, txStatus string
// 		var source, destination, amount, ballot_number, contact_server int
// 		var createdAt string
// 		err := rows.Scan(&txID, &source, &destination, &amount, &ballot_number, &contact_server, &txStatus, &createdAt)
// 		if err != nil {
// 			return nil, err
// 		}
// 		transactions = append(transactions, map[string]interface{}{
// 			"transaction_id": txID,
// 			"source":         source,
// 			"destination":    destination,
// 			"amount":         amount,
// 			"ballot_number":  ballot_number,
// 			"contact_server": contact_server,
// 			"status":         txStatus,
// 			"created_at":     createdAt,
// 		})
// 	}
// 	return transactions, nil
// }

// // Prints all client balances from the database
// func PrintClients(db *sql.DB) error {
// 	rows, err := db.Query(`SELECT client_id, balance FROM clients`)
// 	if err != nil {
// 		return fmt.Errorf("failed to fetch clients: %v", err)
// 	}
// 	defer rows.Close()

// 	fmt.Println("Client Balances:")
// 	for rows.Next() {
// 		var clientID, balance int
// 		err := rows.Scan(&clientID, &balance)
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Printf("  Client ID: %d, Balance: %d\n", clientID, balance)
// 	}
// 	return nil
// }

// Prints all committed transactions
// func PrintDatastore(db *sql.DB) error {
// 	rows, err := db.Query(`
// 		SELECT transaction_id, source, destination, amount, status, created_at
// 		FROM transactions
// 		WHERE status = "committed"
// 		ORDER BY created_at ASC`)
// 	if err != nil {
// 		return fmt.Errorf("failed to fetch committed transactions: %v", err)
// 	}
// 	defer rows.Close()

// 	fmt.Println("Committed Transactions:")
// 	for rows.Next() {
// 		var txID, status, createdAt string
// 		var source, destination, amount int
// 		err := rows.Scan(&txID, &source, &destination, &amount, &status, &createdAt)
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Printf("  [%s] %d -> %d: %d (Status: %s, Created At: %s)\n", txID, source, destination, amount, status, createdAt)
// 	}
// 	return nil
// }

// Sets the lock for a specific client ID
func SetLock(db *sql.DB, clientID int) error {
	_, err := db.Exec(`UPDATE clients SET lock = 1 WHERE client_id = ?`, clientID)
	if err != nil {
		return fmt.Errorf("failed to set lock for client ID %d: %v", clientID, err)
	}
	return nil
}

// Unsets the lock for a specific client ID
func UnsetLock(db *sql.DB, clientID int) error {
	_, err := db.Exec(`UPDATE clients SET lock = 0 WHERE client_id = ?`, clientID)
	if err != nil {
		return fmt.Errorf("failed to unset lock for client ID %d: %v", clientID, err)
	}
	return nil
}

// Checks if a specific client ID is locked
func IsLocked(db *sql.DB, clientID int) (bool, error) {
	var isLocked bool
	row := db.QueryRow(`SELECT lock FROM clients WHERE client_id = ?`, clientID)
	err := row.Scan(&isLocked)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("client ID %d not found", clientID)
		}
		return false, err
	}
	return isLocked, nil
}

// GetAllTransactions retrieves all transactions from the database, ordered by creation time
func GetAllTransactions(db *sql.DB) ([]map[string]interface{}, error) {
	rows, err := db.Query(`
		SELECT transaction_id, source, destination, amount, ballot_number, contact_server, status, created_at
		FROM transactions
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transactions: %v", err)
	}
	defer rows.Close()

	transactions := make([]map[string]interface{}, 0)
	for rows.Next() {
		var txID, status, createdAt string
		var source, destination, amount, ballot_number, contact_server int
		err := rows.Scan(&txID, &source, &destination, &amount, &ballot_number, &contact_server, &status, &createdAt)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, map[string]interface{}{
			"transaction_id": txID,
			"source":         source,
			"destination":    destination,
			"amount":         amount,
			"ballot_number":  ballot_number,
			"contact_server": contact_server,
			"status":         status,
			"created_at":     createdAt,
		})
	}
	return transactions, nil
}

// GetTransaction retrieves a transaction from the database by its ID
func GetTransaction(db *sql.DB, transactionID string) (shared.Transaction, error) {
	var transaction shared.Transaction

	query := `SELECT transaction_id, source, destination, amount, ballot_number, contact_server, status 
			  FROM transactions 
			  WHERE transaction_id = ?`

	row := db.QueryRow(query, transactionID)
	err := row.Scan(
		&transaction.TransactionID,
		&transaction.Source,
		&transaction.Destination,
		&transaction.Amount,
		&transaction.BallotNumber,
		&transaction.ContactServer,
		&transaction.Status,
	)

	if err == sql.ErrNoRows {
		return transaction, fmt.Errorf("transaction not found: %s", transactionID)
	}
	if err != nil {
		return transaction, fmt.Errorf("error fetching transaction: %v", err)
	}

	return transaction, nil
}

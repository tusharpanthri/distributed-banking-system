package shared

// Transaction represents a transaction between two servers
type Transaction struct {
	TransactionID string // Unique transaction ID
	Source        int    // T
	Destination   int    //
	Amount        int    // The amount of the transaction
	Ballot        Ballot // Ballot under which this entry was accepted
	ContactServer int
	Status        string // Status of the transaction (e.g., "pending", "committed","local","failed")
}

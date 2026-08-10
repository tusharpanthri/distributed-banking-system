package shared

type PrepareRequest struct {
	CommittedTransactions []Transaction
	LeaderBallotNumber    int
}

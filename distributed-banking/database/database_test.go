package database

import (
	"os"
	"testing"
)

func TestInitDatabaseCreatesSchemaAndSeedsClients(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)

	d, err := InitDatabase("smoke", []int{1, 2, 3})
	if err != nil {
		t.Fatalf("InitDatabase: %v", err)
	}
	defer d.DB.Close()

	var bal int
	if err := d.DB.QueryRow("SELECT balance FROM clients WHERE client_id = 2").Scan(&bal); err != nil {
		t.Fatalf("query: %v", err)
	}
	if bal != 10 {
		t.Fatalf("balance = %d, want 10", bal)
	}
	if err := ClearTransactions(d.DB); err != nil {
		t.Fatalf("ClearTransactions: %v", err)
	}
}

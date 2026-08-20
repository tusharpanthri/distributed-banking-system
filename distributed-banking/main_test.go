package main

import (
	"fmt"
	"testing"
)

// Every account must land in a cluster that exists, for any cluster count --
// not only counts that divide the account total evenly. The previous flat
// dataCount/clusterCount stride mapped the trailing accounts past the end.
func TestAssignShardsToClustersCoversEveryAccount(t *testing.T) {
	const accounts = 3000

	for _, clusterCount := range []int{1, 2, 3, 4, 7, 9, 16, 100, 2999} {
		t.Run(fmt.Sprintf("clusters=%d", clusterCount), func(t *testing.T) {
			mapping := AssignShardsToClusters(accounts, clusterCount)

			if len(mapping) != accounts {
				t.Fatalf("mapped %d accounts, want %d", len(mapping), accounts)
			}

			valid := make(map[string]bool, clusterCount)
			for i := 1; i <= clusterCount; i++ {
				valid[fmt.Sprintf("C%d", i)] = true
			}

			sizes := make(map[string]int, clusterCount)
			for account := 1; account <= accounts; account++ {
				cluster, ok := mapping[account]
				if !ok {
					t.Fatalf("account %d was not assigned to any cluster", account)
				}
				if !valid[cluster] {
					t.Fatalf("account %d assigned to %s, which does not exist", account, cluster)
				}
				sizes[cluster]++
			}

			// Ranges should differ by at most one account.
			min, max := accounts, 0
			for _, size := range sizes {
				if size < min {
					min = size
				}
				if size > max {
					max = size
				}
			}
			if max-min > 1 {
				t.Errorf("cluster sizes range from %d to %d, want a difference of at most 1", min, max)
			}
		})
	}
}

func TestAssignShardsToClustersIsContiguous(t *testing.T) {
	mapping := AssignShardsToClusters(10, 3)

	// 10 accounts over 3 clusters: 4, 3, 3.
	want := map[int]string{
		1: "C1", 2: "C1", 3: "C1", 4: "C1",
		5: "C2", 6: "C2", 7: "C2",
		8: "C3", 9: "C3", 10: "C3",
	}
	for account, cluster := range want {
		if got := mapping[account]; got != cluster {
			t.Errorf("account %d mapped to %s, want %s", account, got, cluster)
		}
	}
}

func TestAssignShardsToClustersHandlesDegenerateInput(t *testing.T) {
	if got := AssignShardsToClusters(100, 0); len(got) != 0 {
		t.Errorf("zero clusters produced %d assignments", len(got))
	}
	if got := AssignShardsToClusters(0, 3); len(got) != 0 {
		t.Errorf("zero accounts produced %d assignments", len(got))
	}
}

package shared

var serversPerCluster int

func SetServersPerCluster(count int) {
	serversPerCluster = count
}

func GetServersPerCluster() int {
	return serversPerCluster
}

// GetQuorumSize returns the size of a strict majority of the cluster. The
// previous (n+1)/2 form is only a majority for odd n: with four servers it
// returned 2, which lets two disjoint halves of the cluster each believe they
// hold a quorum.
func GetQuorumSize() int {
	return serversPerCluster/2 + 1
}

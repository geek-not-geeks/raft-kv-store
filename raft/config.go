package raft

// Peer describes one server in the cluster.
type Peer struct {
	ID      string
	Address string // e.g. "localhost:8001"
}

// ClusterConfig is the full list of servers that make up the cluster.
var ClusterConfig = []Peer{
	{ID: "node1", Address: "localhost:8001"},
	{ID: "node2", Address: "localhost:8002"},
	{ID: "node3", Address: "localhost:8003"},
}

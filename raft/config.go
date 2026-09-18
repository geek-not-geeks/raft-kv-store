package raft

type Peer struct {
	ID            string
	ClientAddress string // SET/GET/DELETE clients connect here
	RaftAddress   string // other Raft nodes talk to this node here (internal only)
}

var ClusterConfig = []Peer{
	{ID: "node1", ClientAddress: "localhost:8001", RaftAddress: "localhost:9001"},
	{ID: "node2", ClientAddress: "localhost:8002", RaftAddress: "localhost:9002"},
	{ID: "node3", ClientAddress: "localhost:8003", RaftAddress: "localhost:9003"},
}

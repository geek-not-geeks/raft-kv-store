package raft

import (
	"encoding/json"
	"net"
	"time"
)

type RequestVoteArgs struct {
	Term        int
	CandidateID string
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term    int
	Command string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Used only when Success is false, so the leader can jump back
	// directly to the right point instead of retrying one index at a time.
	ConflictIndex int
	ConflictTerm  int
}

type rpcEnvelope struct {
	Kind string          `json:"kind"`
	Body json.RawMessage `json:"body"`
}

func sendRPC(address string, kind string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(500 * time.Millisecond))

	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(conn).Encode(rpcEnvelope{Kind: kind, Body: body}); err != nil {
		return err
	}
	return json.NewDecoder(conn).Decode(reply)
}

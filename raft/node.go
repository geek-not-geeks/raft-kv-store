package raft

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

type Node struct {
	mu sync.Mutex

	ID    string
	Peers []Peer

	CurrentTerm int
	VotedFor    string
	State       State
	LeaderID    string

	Log         []LogEntry
	CommitIndex int
	LastApplied int

	NextIndex  map[string]int
	MatchIndex map[string]int

	resetElectionTimer chan bool
	Partitioned        bool

	ApplyFn func(command string)
}

func NewNode(id string, peers []Peer) *Node {
	return &Node{
		ID:                 id,
		Peers:              peers,
		State:              Follower,
		Log:                []LogEntry{{Term: 0, Command: ""}},
		resetElectionTimer: make(chan bool, 1),
	}
}

func (n *Node) SetPartitioned(p bool) {
	n.mu.Lock()
	n.Partitioned = p
	n.mu.Unlock()
}

func (n *Node) IsPartitioned() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Partitioned
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) RunElectionTimer() {
	for {
		timeout := randomElectionTimeout()
		select {
		case <-time.After(timeout):
			n.mu.Lock()
			isLeader := n.State == Leader
			n.mu.Unlock()
			if isLeader {
				continue
			}
			n.startElection()
		case <-n.resetElectionTimer:
			continue
		}
	}
}

func (n *Node) ResetTimer() {
	select {
	case n.resetElectionTimer <- true:
	default:
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.Partitioned {
		n.mu.Unlock()
		return
	}
	n.State = Candidate
	n.CurrentTerm++
	currentTerm := n.CurrentTerm
	n.VotedFor = n.ID
	n.mu.Unlock()

	fmt.Printf("[%s] election timeout — starting election for term %d\n", n.ID, currentTerm)

	var votesMu sync.Mutex
	votes := 1
	decided := false

	for _, peer := range n.Peers {
		if peer.ID == n.ID {
			continue
		}
		go func(p Peer) {
			args := RequestVoteArgs{Term: currentTerm, CandidateID: n.ID}
			var reply RequestVoteReply
			if err := sendRPC(p.RaftAddress, "RequestVote", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.CurrentTerm {
				n.CurrentTerm = reply.Term
				n.State = Follower
				n.VotedFor = ""
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			votesMu.Lock()
			defer votesMu.Unlock()
			if decided || !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(n.Peers)/2 {
				decided = true
				n.becomeLeader(currentTerm)
			}
		}(peer)
	}
}

func (n *Node) becomeLeader(term int) {
	n.mu.Lock()
	if n.State != Candidate || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	n.State = Leader
	n.LeaderID = n.ID
	n.NextIndex = make(map[string]int)
	n.MatchIndex = make(map[string]int)
	lastLogIndex := len(n.Log) - 1
	for _, peer := range n.Peers {
		if peer.ID == n.ID {
			continue
		}
		n.NextIndex[peer.ID] = lastLogIndex + 1
		n.MatchIndex[peer.ID] = 0
	}
	n.mu.Unlock()

	fmt.Printf("[%s] *** became LEADER for term %d ***\n", n.ID, term)
	go n.leaderReplicationLoop(term)
}

func (n *Node) leaderReplicationLoop(term int) {
	for {
		n.mu.Lock()
		stillLeader := n.State == Leader && n.CurrentTerm == term
		n.mu.Unlock()
		if !stillLeader {
			return
		}
		for _, peer := range n.Peers {
			if peer.ID == n.ID {
				continue
			}
			go n.replicateToPeer(peer, term)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) replicateToPeer(peer Peer, term int) {
	n.mu.Lock()
	if n.Partitioned {
		n.mu.Unlock()
		return
	}
	if n.State != Leader || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.NextIndex[peer.ID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(n.Log) {
		prevLogTerm = n.Log[prevLogIndex].Term
	}
	var entries []LogEntry
	if nextIdx < len(n.Log) {
		entries = append(entries, n.Log[nextIdx:]...)
	}
	args := AppendEntriesArgs{
		Term: term, LeaderID: n.ID,
		PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
		Entries: entries, LeaderCommit: n.CommitIndex,
	}
	n.mu.Unlock()

	var reply AppendEntriesReply
	if err := sendRPC(peer.RaftAddress, "AppendEntries", args, &reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.CurrentTerm {
		n.CurrentTerm = reply.Term
		n.State = Follower
		n.VotedFor = ""
		return
	}
	if n.State != Leader || n.CurrentTerm != term {
		return
	}

	if reply.Success {
		n.MatchIndex[peer.ID] = prevLogIndex + len(args.Entries)
		n.NextIndex[peer.ID] = n.MatchIndex[peer.ID] + 1
		n.advanceCommitIndex(term)
	} else if n.NextIndex[peer.ID] > 1 {
		n.NextIndex[peer.ID]--
	}
}

func (n *Node) advanceCommitIndex(term int) {
	for N := len(n.Log) - 1; N > n.CommitIndex; N-- {
		if n.Log[N].Term != term {
			continue
		}
		count := 1
		for _, peer := range n.Peers {
			if peer.ID != n.ID && n.MatchIndex[peer.ID] >= N {
				count++
			}
		}
		if count > len(n.Peers)/2 {
			n.CommitIndex = N
			n.applyCommitted()
			return
		}
	}
}

func (n *Node) applyCommitted() {
	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		entry := n.Log[n.LastApplied]
		if n.ApplyFn != nil && entry.Command != "" {
			n.ApplyFn(entry.Command)
		}
	}
}

func (n *Node) Propose(command string) error {
	n.mu.Lock()
	if n.Partitioned {
		n.mu.Unlock()
		return fmt.Errorf("this node is partitioned (simulated network cut)")
	}
	if n.State != Leader {
		leader := n.LeaderID
		n.mu.Unlock()
		if leader == "" {
			return fmt.Errorf("no known leader right now, try again shortly")
		}
		return fmt.Errorf("not leader, leader is %s", leader)
	}
	n.Log = append(n.Log, LogEntry{Term: n.CurrentTerm, Command: command})
	index := len(n.Log) - 1
	n.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		committed := n.CommitIndex >= index
		stillLeader := n.State == Leader
		n.mu.Unlock()
		if committed {
			return nil
		}
		if !stillLeader {
			return fmt.Errorf("lost leadership before command committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for commit")
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	if n.Partitioned {
		term := n.CurrentTerm
		n.mu.Unlock()
		return RequestVoteReply{Term: term, VoteGranted: false}
	}
	defer n.mu.Unlock()

	if args.Term < n.CurrentTerm {
		return RequestVoteReply{Term: n.CurrentTerm, VoteGranted: false}
	}
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
		n.State = Follower
	}
	granted := false
	if n.VotedFor == "" || n.VotedFor == args.CandidateID {
		n.VotedFor = args.CandidateID
		granted = true
		n.ResetTimer()
	}
	fmt.Printf("[%s] vote request from %s (term %d) — granted: %t\n", n.ID, args.CandidateID, args.Term, granted)
	return RequestVoteReply{Term: n.CurrentTerm, VoteGranted: granted}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	if n.Partitioned {
		term := n.CurrentTerm
		n.mu.Unlock()
		return AppendEntriesReply{Term: term, Success: false}
	}
	defer n.mu.Unlock()

	if args.Term < n.CurrentTerm {
		return AppendEntriesReply{Term: n.CurrentTerm, Success: false}
	}
	if n.LeaderID != args.LeaderID || n.State != Follower || args.Term > n.CurrentTerm {
		fmt.Printf("[%s] recognizing %s as leader (term %d)\n", n.ID, args.LeaderID, args.Term)
	}
	n.CurrentTerm = args.Term
	n.State = Follower
	n.LeaderID = args.LeaderID
	n.ResetTimer()

	if args.PrevLogIndex >= len(n.Log) {
		return AppendEntriesReply{Term: n.CurrentTerm, Success: false}
	}
	if args.PrevLogIndex >= 0 && n.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return AppendEntriesReply{Term: n.CurrentTerm, Success: false}
	}

	if len(args.Entries) > 0 {
		n.Log = append(n.Log[:args.PrevLogIndex+1], args.Entries...)
		var cmds []string
		for _, e := range args.Entries {
			cmds = append(cmds, e.Command)
		}
		fmt.Printf("[%s] appended %d entr(ies): %s\n", n.ID, len(args.Entries), strings.Join(cmds, " | "))
	}

	if args.LeaderCommit > n.CommitIndex {
		newCommit := args.LeaderCommit
		if lastIdx := len(n.Log) - 1; newCommit > lastIdx {
			newCommit = lastIdx
		}
		n.CommitIndex = newCommit
		n.applyCommitted()
	}
	return AppendEntriesReply{Term: n.CurrentTerm, Success: true}
}

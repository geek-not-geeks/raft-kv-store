package raft

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
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

	resetElectionTimer chan bool
}

func NewNode(id string, peers []Peer) *Node {
	return &Node{
		ID:                 id,
		Peers:              peers,
		CurrentTerm:        0,
		State:              Follower,
		resetElectionTimer: make(chan bool, 1),
	}
}

func randomElectionTimeout() time.Duration {
	ms := 150 + rand.Intn(150) // 150-299ms
	return time.Duration(ms) * time.Millisecond
}

// RunElectionTimer should be started once, in a goroutine, when a node boots.
func (n *Node) RunElectionTimer() {
	for {
		timeout := randomElectionTimeout()
		select {
		case <-time.After(timeout):
			n.mu.Lock()
			isLeader := n.State == Leader
			n.mu.Unlock()
			if isLeader {
				// Leaders don't run elections against themselves. A leader
				// only steps down when it learns of a higher term from
				// someone else's message (handled in HandleHeartbeat /
				// HandleRequestVote below).
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
			granted := n.sendRequestVote(p, currentTerm)

			votesMu.Lock()
			defer votesMu.Unlock()
			if decided || !granted {
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

func (n *Node) sendRequestVote(peer Peer, term int) bool {
	conn, err := net.DialTimeout("tcp", peer.Address, 300*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	msg := fmt.Sprintf("REQUESTVOTE %d %s\n", term, n.ID)
	conn.Write([]byte(msg))

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	response = strings.TrimSpace(response)
	parts := strings.Split(response, " ")
	if len(parts) != 3 || parts[0] != "VOTE" {
		return false
	}
	return parts[2] == "true"
}

func (n *Node) becomeLeader(term int) {
	n.mu.Lock()
	if n.State != Candidate || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	n.State = Leader
	n.LeaderID = n.ID
	n.mu.Unlock()

	fmt.Printf("[%s] *** became LEADER for term %d ***\n", n.ID, term)
	go n.leaderHeartbeatLoop(term)
}

func (n *Node) leaderHeartbeatLoop(term int) {
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
			go n.sendHeartbeat(peer, term)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) sendHeartbeat(peer Peer, term int) {
	conn, err := net.DialTimeout("tcp", peer.Address, 300*time.Millisecond)
	if err != nil {
		return
	}
	defer conn.Close()
	msg := fmt.Sprintf("HEARTBEAT %d %s\n", term, n.ID)
	conn.Write([]byte(msg))
}

func (n *Node) HandleRequestVote(term int, candidateID string) (int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term < n.CurrentTerm {
		return n.CurrentTerm, false
	}
	if term > n.CurrentTerm {
		n.CurrentTerm = term
		n.VotedFor = ""
		n.State = Follower
	}

	granted := false
	if n.VotedFor == "" || n.VotedFor == candidateID {
		n.VotedFor = candidateID
		granted = true
		n.ResetTimer()
	}
	fmt.Printf("[%s] vote request from %s (term %d) — granted: %t\n", n.ID, candidateID, term, granted)
	return n.CurrentTerm, granted
}

func (n *Node) HandleHeartbeat(term int, leaderID string) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term < n.CurrentTerm {
		return n.CurrentTerm
	}
	if n.LeaderID != leaderID || n.State != Follower {
		fmt.Printf("[%s] recognizing %s as leader (term %d)\n", n.ID, leaderID, term)
	}
	n.CurrentTerm = term
	n.State = Follower
	n.LeaderID = leaderID
	n.ResetTimer()
	return n.CurrentTerm
}

func ParseInt(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

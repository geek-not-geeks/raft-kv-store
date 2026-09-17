package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/geeknotgeeks/raft-kv-store/raft"
)

var myID string
var myAddress string

// The in-memory key-value store, same as Phase 1.
var (
	store = make(map[string]string)
	mu    sync.Mutex
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run cmd/server/main.go <nodeID>")
		return
	}
	myID = os.Args[1]

	for _, peer := range raft.ClusterConfig {
		if peer.ID == myID {
			myAddress = peer.Address
		}
	}
	if myAddress == "" {
		fmt.Println("Unknown node ID:", myID)
		return
	}

	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		fmt.Println("Failed to start:", err)
		return
	}
	defer listener.Close()
	fmt.Printf("[%s] listening on %s\n", myID, myAddress)

	go heartbeatLoop()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn)
	}
}

func heartbeatLoop() {
	for {
		for _, peer := range raft.ClusterConfig {
			if peer.ID == myID {
				continue
			}
			go pingPeer(peer)
		}
		time.Sleep(2 * time.Second)
	}
}

func pingPeer(peer raft.Peer) {
	conn, err := net.DialTimeout("tcp", peer.Address, 500*time.Millisecond)
	if err != nil {
		fmt.Printf("[%s] cannot reach %s\n", myID, peer.ID)
		return
	}
	defer conn.Close()
	conn.Write(fmt.Appendf(nil, "PING from %s\n", myID))
	fmt.Printf("[%s] pinged %s successfully\n", myID, peer.ID)
}

// handleConnection is the Phase 1 logic: reads commands line by line
// and responds to SET / GET / DELETE. PING messages from peers also
// land here (via pingPeer above) and just fall into the default case.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		command := strings.ToUpper(parts[0])

		var response string
		switch command {
		case "SET":
			if len(parts) < 3 {
				response = "ERROR: usage is SET key value"
			} else {
				mu.Lock()
				store[parts[1]] = parts[2]
				mu.Unlock()
				response = "OK"
			}
		case "GET":
			if len(parts) < 2 {
				response = "ERROR: usage is GET key"
			} else {
				mu.Lock()
				value, exists := store[parts[1]]
				mu.Unlock()
				if exists {
					response = value
				} else {
					response = "ERROR: key not found"
				}
			}
		case "DELETE":
			if len(parts) < 2 {
				response = "ERROR: usage is DELETE key"
			} else {
				mu.Lock()
				delete(store, parts[1])
				mu.Unlock()
				response = "OK"
			}
		case "PING":
			// A peer's heartbeat landed here — nothing to respond with,
			// no client is waiting on the other end for a reply.
			continue
		default:
			response = "ERROR: unknown command"
		}

		conn.Write(fmt.Appendf(nil, "%s\n", response))
	}
}

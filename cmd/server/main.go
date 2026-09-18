package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/geeknotgeeks/raft-kv-store/raft"
)

var (
	store = make(map[string]string)
	mu    sync.Mutex
	node  *raft.Node
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run cmd/server/main.go <nodeID>")
		return
	}
	myID := os.Args[1]

	var myAddress string
	for _, peer := range raft.ClusterConfig {
		if peer.ID == myID {
			myAddress = peer.Address
		}
	}
	if myAddress == "" {
		fmt.Println("Unknown node ID:", myID)
		return
	}

	node = raft.NewNode(myID, raft.ClusterConfig)

	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		fmt.Println("Failed to start:", err)
		return
	}
	defer listener.Close()
	fmt.Printf("[%s] listening on %s\n", myID, myAddress)

	go node.RunElectionTimer()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn)
	}
}

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
		case "REQUESTVOTE":
			// format: REQUESTVOTE <term> <candidateID>
			term := raft.ParseInt(parts[1])
			candidateID := parts[2]
			currentTerm, granted := node.HandleRequestVote(term, candidateID)
			response = fmt.Sprintf("VOTE %d %t", currentTerm, granted)
		case "HEARTBEAT":
			// format: HEARTBEAT <term> <leaderID>
			term := raft.ParseInt(parts[1])
			leaderID := parts[2]
			currentTerm := node.HandleHeartbeat(term, leaderID)
			response = fmt.Sprintf("ACK %d", currentTerm)
		default:
			response = "ERROR: unknown command"
		}

		conn.Write([]byte(response + "\n"))
	}
}

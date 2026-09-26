package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/geek-not-geeks/raft-kv-store/raft"
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

	var clientAddress, raftAddress string
	for _, peer := range raft.ClusterConfig {
		if peer.ID == myID {
			clientAddress = peer.ClientAddress
			raftAddress = peer.RaftAddress
		}
	}
	if clientAddress == "" {
		fmt.Println("Unknown node ID:", myID)
		return
	}

	node = raft.NewNode(myID, raft.ClusterConfig)
	node.ApplyFn = applyToStore

	clientListener, err := net.Listen("tcp", clientAddress)
	if err != nil {
		fmt.Println("Failed to start client listener:", err)
		return
	}
	defer clientListener.Close()

	raftListener, err := net.Listen("tcp", raftAddress)
	if err != nil {
		fmt.Println("Failed to start raft listener:", err)
		return
	}
	defer raftListener.Close()

	fmt.Printf("[%s] client port %s, raft port %s\n", myID, clientAddress, raftAddress)

	go node.RunElectionTimer()
	go acceptLoop(raftListener, handleRaftConnection)
	acceptLoop(clientListener, handleClientConnection)
}

func acceptLoop(listener net.Listener, handler func(net.Conn)) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handler(conn)
	}
}

func applyToStore(command string) {
	parts := strings.SplitN(command, " ", 3)
	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) == 3 {
			mu.Lock()
			store[parts[1]] = parts[2]
			mu.Unlock()
		}
	case "DELETE":
		if len(parts) >= 2 {
			mu.Lock()
			delete(store, parts[1])
			mu.Unlock()
		}
	}
}

func handleClientConnection(conn net.Conn) {
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
			} else if err := node.Propose(line); err != nil {
				response = "ERROR: " + err.Error()
			} else {
				response = "OK"
			}
		case "DELETE":
			if len(parts) < 2 {
				response = "ERROR: usage is DELETE key"
			} else if err := node.Propose(line); err != nil {
				response = "ERROR: " + err.Error()
			} else {
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
		case "PARTITION":
			if len(parts) < 2 {
				response = "ERROR: usage is PARTITION on|off"
			} else if parts[1] == "on" {
				node.SetPartitioned(true)
				response = "OK - simulating network partition"
			} else {
				node.SetPartitioned(false)
				response = "OK - partition healed"
			}
		default:
			response = "ERROR: unknown command"
		}
		conn.Write([]byte(response + "\n"))
	}
}

type rpcEnvelope struct {
	Kind string          `json:"kind"`
	Body json.RawMessage `json:"body"`
}

func handleRaftConnection(conn net.Conn) {
	defer conn.Close()
	var env rpcEnvelope
	if err := json.NewDecoder(conn).Decode(&env); err != nil {
		return
	}
	switch env.Kind {
	case "RequestVote":
		var args raft.RequestVoteArgs
		json.Unmarshal(env.Body, &args)
		json.NewEncoder(conn).Encode(node.HandleRequestVote(args))
	case "AppendEntries":
		var args raft.AppendEntriesArgs
		json.Unmarshal(env.Body, &args)
		json.NewEncoder(conn).Encode(node.HandleAppendEntries(args))
	}
}

package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

// This is our actual "database" for now — just a map living in memory.
// A sync.Mutex is a lock: since multiple clients could connect at once,
// we need to make sure two of them never write to the map at the exact
// same instant, which could corrupt it. The mutex prevents that.
var (
	store = make(map[string]string)
	mu    sync.Mutex
)

func main() {
	// Start listening for incoming connections on port 8001.
	listener, err := net.Listen("tcp", ":8001")
	if err != nil {
		fmt.Println("Failed to start server:", err)
		return
	}
	defer listener.Close()
	fmt.Println("Server listening on port 8001...")

	// Loop forever: each time someone connects, handle them.
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		// "go" here means: handle this connection in the background,
		// so the server can immediately go back to accepting the NEXT
		// client too, instead of making everyone wait in line.
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Read one line of text the client sent (commands end in a newline).
		line, err := reader.ReadString('\n')
		if err != nil {
			return // client disconnected
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
		default:
			response = "ERROR: unknown command"
		}

		conn.Write([]byte(response + "\n"))
	}
}

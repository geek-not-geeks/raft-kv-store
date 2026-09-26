package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

var addresses = map[string]string{
	"node1": "localhost:8001",
	"node2": "localhost:8002",
	"node3": "localhost:8003",
}

func main() {
	results := make(map[string]string)
	for id, addr := range addresses {
		results[id] = dump(addr)
	}

	for id, data := range results {
		fmt.Printf("%s: %s\n", id, data)
	}

	first := ""
	match := true
	for _, data := range results {
		if first == "" {
			first = data
			continue
		}
		if data != first {
			match = false
		}
	}

	if match {
		fmt.Println("\nPASS — all nodes have identical data")
	} else {
		fmt.Println("\nFAIL — nodes disagree, see above")
	}
}

func dump(address string) string {
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		return "UNREACHABLE"
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write([]byte("DUMP\n"))
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "ERROR"
	}
	return response
}

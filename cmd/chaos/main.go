package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var addresses = []string{"localhost:8001", "localhost:8002", "localhost:8003"}

func main() {
	durationSec := 30
	if len(os.Args) > 1 {
		durationSec, _ = strconv.Atoi(os.Args[1])
	}

	currentLeader := addresses[0]
	successCount := 0
	failCount := 0
	deadline := time.Now().Add(time.Duration(durationSec) * time.Second)

	for time.Now().Before(deadline) {
		key := fmt.Sprintf("k%d", rand.Intn(10))
		value := strconv.Itoa(rand.Intn(100000))
		cmd := fmt.Sprintf("SET %s %s\n", key, value)

		ok, newLeader := trySet(currentLeader, cmd)
		if ok {
			successCount++
		} else {
			failCount++
			if newLeader != "" {
				currentLeader = newLeader
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Printf("\nDone. Successful writes: %d, failed attempts: %d\n", successCount, failCount)
}

// trySet sends one SET to the given address. Returns success, and if the
// response told us who the real leader is, returns that address too.
func trySet(address string, cmd string) (bool, string) {
	conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
	if err != nil {
		return false, randomOtherAddress(address)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(500 * time.Millisecond))

	conn.Write([]byte(cmd))
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return false, randomOtherAddress(address)
	}
	response = strings.TrimSpace(response)

	if response == "OK" {
		return true, ""
	}
	// Expected failure format: "ERROR: not leader, leader is node1"
	if strings.Contains(response, "leader is") {
		parts := strings.Split(response, "leader is ")
		leaderID := strings.TrimSpace(parts[len(parts)-1])
		return false, nodeIDToAddress(leaderID)
	}
	return false, ""
}

func nodeIDToAddress(id string) string {
	switch id {
	case "node1":
		return "localhost:8001"
	case "node2":
		return "localhost:8002"
	case "node3":
		return "localhost:8003"
	}
	return ""
}

func randomOtherAddress(current string) string {
	return addresses[rand.Intn(len(addresses))]
}

package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	leader := findLeader()
	if leader == "" {
		fmt.Println("Could not find a leader among localhost:8001/8002/8003 - is the cluster running?")
		return
	}
	fmt.Println("Leader found at:", leader)

	const numWorkers = 20
	const durationSec = 10

	var successCount int64
	var failCount int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", leader)
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				cmd := fmt.Sprintf("SET bench%d_%d %d\n", workerID, n, n)
				conn.Write([]byte(cmd))
				resp, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if strings.TrimSpace(resp) == "OK" {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&failCount, 1)
				}
				n++
			}
		}(i)
	}

	time.Sleep(durationSec * time.Second)
	close(stop)
	wg.Wait()

	total := successCount + failCount
	fmt.Printf("\n--- Throughput ---\n")
	fmt.Printf("Duration: %ds, Workers: %d\n", durationSec, numWorkers)
	fmt.Printf("Successful writes: %d, Failed: %d, Total: %d\n", successCount, failCount, total)
	fmt.Printf("Writes/sec: %.1f\n", float64(successCount)/float64(durationSec))

	measureFailover()
}

func findLeader() string {
	addrs := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	for _, addr := range addrs {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err != nil {
			continue
		}
		conn.Write([]byte("SET __probe__ 1\n"))
		resp, _ := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if strings.TrimSpace(resp) == "OK" {
			return addr
		}
	}
	return ""
}

func measureFailover() {
	fmt.Println("\n--- Failover timing ---")

	currentLeader := findLeader()
	if currentLeader == "" {
		fmt.Println("No leader found, skipping failover test")
		return
	}
	fmt.Println("Killing leader at:", currentLeader)

	conn, err := net.Dial("tcp", currentLeader)
	if err != nil {
		fmt.Println("Could not connect to send KILL:", err)
		return
	}
	start := time.Now()
	conn.Write([]byte("KILL\n"))
	conn.Close()

	addrs := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	for {
		for _, addr := range addrs {
			if addr == currentLeader {
				continue
			}
			c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err != nil {
				continue
			}
			c.Write([]byte("SET __probe2__ 1\n"))
			resp, _ := bufio.NewReader(c).ReadString('\n')
			c.Close()
			if strings.TrimSpace(resp) == "OK" {
				fmt.Printf("New leader ready at %s after %v\n", addr, time.Since(start))
				return
			}
		}
		if time.Since(start) > 10*time.Second {
			fmt.Println("Gave up waiting after 10s")
			return
		}
	}
}

var _ = strconv.Itoa // unused import guard removed below if needed

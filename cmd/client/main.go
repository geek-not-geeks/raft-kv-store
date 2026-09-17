package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8001")
	if err != nil {
		fmt.Println("Could not connect to server:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to KV store. Type commands like: SET x 5")
	scanner := bufio.NewScanner(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()

		conn.Write([]byte(input + "\n"))

		response, err := serverReader.ReadString('\n')
		if err != nil {
			fmt.Println("Server disconnected.")
			return
		}
		fmt.Print(response)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}

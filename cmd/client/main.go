package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	address := "localhost:8001"
	if len(os.Args) > 1 {
		address = os.Args[1]
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Could not connect to server:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to", address, "— if you get 'not leader', reconnect using the address it names")
	scanner := bufio.NewScanner(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		conn.Write([]byte(scanner.Text() + "\n"))
		response, err := serverReader.ReadString('\n')
		if err != nil {
			fmt.Println("Server disconnected.")
			return
		}
		fmt.Print(response)
	}
}

package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	serverIPAddr := "127.0.0.1"
	serverPort := "8008"

	// 1. Bind to a local port and listen for incoming TCP traffic:
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", serverPort, err)
	}
	defer listener.Close()

	fmt.Printf("Server is listening on %s...", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))

	// 2. Acceptance loop:
	for {
		// Accept blocks execution until a client connects:
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		fmt.Printf("Client connected from: %s\n", conn.RemoteAddr().String())

		// 3. Read data from the client
		handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close() // Automatically close the socket when this function ends

	buffer := make([]byte, 1024) // Create a 1KB buffer for reading data

	for {
		// Read blocks execution until the blient sends data:
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr().String())
			return
		}

		// Print the raw string message we received:
		fmt.Printf("Received Ticker Data: %s\n", string(buffer[:bytesRead]))
	}
}

package main

import (
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

func main() {

	var lock sync.Mutex

	producers := make(map[string][]string)

	consumers := make(map[string][]string)

	// Listen for connections

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Print(err)
		}
		go handleConnection(connection, producers, consumers, &lock)
	}
}

func handleConnection(connection net.Conn, producers, consumers map[string][]string, lock *sync.Mutex) {

	ipString := strings.SplitAfter(connection.RemoteAddr().String(), ":")

	log.Print("Received data from ", ipString)

	data := make([]byte, 2048)

	byteCount, err := connection.Read(data)

	if err != nil {
		log.Print(err)
	}

	log.Print("Received ", byteCount, " bytes. Data is "+string(data[:byteCount]))

	message := strings.Fields(string(data[:byteCount]))

	if message[0] == "exit" {

		log.Print("Got exit command")
	}

	if message[0] == "producer" {

		log.Print("Got a producer! It will be listening on port " + message[1])

		lock.Lock()

		producers["topic"] = append(producers["topic"], ipString[0]+message[1])

		lock.Unlock()
	}

	if message[0] == "consumer" {

		lock.Lock()

		consumers["topic"] = append(consumers["topic"], ipString[0]+message[1])

		lock.Unlock()

		connection.Write([]byte(strconv.Itoa(len(producers))))

		for _, producerData := range producers["topic"] {

			connection.Write([]byte(producerData))
		}
	}

	connection.Write([]byte("Got your message!"))

	connection.Close()
}

package main

import (
	"log"
	"net"
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

	defer connection.Close()

	ipString := strings.SplitAfter(connection.RemoteAddr().String(), ":")

	log.Print("Received data from ", ipString[0])

	data := make([]byte, 2048)

	byteCount, err := connection.Read(data)

	if err != nil {

		log.Print(err)

		return
	}

	log.Printf("Received %d bytes. Data is %s", byteCount, string(data[:byteCount]))

	message := strings.Fields(string(data[:byteCount]))

	if message[0] == "exit" {

		log.Print("Got exit command")

		lock.Lock()

		for index, producerID := range producers["topic"] {

			if producerID == ipString[0]+message[1] {

				producers["topic"] = append(producers["topic"][:index], producers["topic"][index+1:]...)

				break
			}
		}

		lock.Unlock()
	}

	if message[0] == "producer" {

		log.Print("Got a producer! It will be listening on port " + message[1])

		lock.Lock()

		producers["topic"] = append(producers["topic"], ipString[0]+message[1])

		for _, consumer := range consumers["topic"] {

			go sendConsumerNewProducer(consumer, ipString[0]+message[1])
		}

		lock.Unlock()
	}

	if message[0] == "consumer" {

		lock.Lock()

		consumers["topic"] = append(consumers["topic"], ipString[0]+message[1])

		lock.Unlock()

		var producerList string

		for _, producerData := range producers["topic"] {

			producerList += producerData + "\n"
		}

		connection.Write([]byte(producerList))

		byteCount, err = connection.Read(data)

		if err != nil {

			log.Print(err)
		}

		log.Print(string(data[:byteCount]))
	}

	connection.Write([]byte("Got your message!"))
}

func sendConsumerNewProducer(consumer, producer string) {

	log.Print("Sending producer: ", producer, " to consumer: ", consumer)

	connection, err := net.Dial("tcp", consumer)

	if err != nil {

		log.Print(err)

		return
	}

	connection.Write([]byte(producer + "\n"))

	connection.Close()
}

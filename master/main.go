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

	defer connection.Write([]byte("Got your message!"))

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

	if message[0] == "producer" {

		log.Print("Got thing from producer")

		lock.Lock()

		defer lock.Unlock()

		if message[1] == "exit" {

			log.Print("Got exit command from producer")

			for index, producerID := range producers[message[2]] {

				if producerID == ipString[0]+message[3] {

					producers[message[2]] = append(producers[message[2]][:index], producers[message[2]][index+1:]...)

					break
				}
			}

			return
		}

		producers[message[2]] = append(producers[message[2]], ipString[0]+message[1])

		for _, consumer := range consumers[message[2]] {

			go sendConsumerNewProducer(consumer, ipString[0]+message[1])
		}

		return
	}

	if message[0] == "consumer" {

		lock.Lock()

		defer lock.Unlock()

		log.Print("Got thing from consumer")

		if message[1] == "exit" {

			log.Print("Got exit notification from consumer")

			for index := range consumers[message[2]] {

				if consumers[message[2]][index] == ipString[0]+message[3] {

					consumers[message[2]] = append(consumers[message[2]][:index], consumers[message[2]][index+1:]...)
				}
			}

			return
		}

		log.Print("Consumer message: ", message)

		consumers[message[2]] = append(consumers[message[2]], ipString[0]+message[1])

		log.Print("Consumer list: ", consumers)

		var producerList string

		for _, producerData := range producers[message[2]] {

			producerList += producerData + "\n"
		}

		connection.Write([]byte(producerList))

		byteCount, err = connection.Read(data)

		if err != nil {

			log.Print("ERROR on connection read:", err)
		}

		log.Print("Consumer response: ", string(data[:byteCount]))
	}
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

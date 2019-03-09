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

	lock.Lock()

	defer lock.Unlock()

	defer connection.Close()

	defer connection.Write([]byte("Got your message!"))

	ipString := strings.SplitAfter(connection.RemoteAddr().String(), ":")[0]

	log.Print("Received data from ", ipString)

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

		if message[1] == "exit" {

			topic := message[2]

			port := message[3]

			log.Print("Got exit command from producer")

			for index, producerID := range producers[topic] {

				if producerID == ipString+port {

					producers[topic] = append(producers[topic][:index], producers[topic][index+1:]...)

					break
				}
			}

			return
		}

		topic := message[2]

		port := message[1]

		log.Print("Adding new producer")

		producers[topic] = append(producers[topic], ipString+port)

		log.Print("There are now ", len(producers[topic]), " for topic: ", topic)

		for _, consumer := range consumers[topic] {

			log.Print("Sending new producer to ", consumer)

			go sendConsumerNewProducer(consumer, ipString+port)
		}

		return
	}

	if message[0] == "consumer" {

		log.Print("Got thing from consumer")

		log.Print("Consumer message: ", message)

		if message[1] == "exit" {

			topic := message[2]

			port := message[3]

			log.Print("Got exit notification from consumer")

			for index := range consumers[topic] {

				if consumers[topic][index] == ipString+port {

					consumers[topic] = append(consumers[topic][:index], consumers[topic][index+1:]...)

					break
				}
			}

			return
		}

		port := message[1]

		topic := message[2]

		consumers[topic] = append(consumers[topic], ipString+port)

		log.Print("Consumer list: ", consumers)

		producerList := ""

		log.Print("Number of producers being sent ", len(producers[topic]), "for topic: ", topic)

		for _, producerData := range producers[topic] {

			log.Print("Adding ", producerData, " to producer list")

			producerList += producerData + "\n"
		}

		if producerList == "" {

			producerList = "\n"
		}

		log.Print("Sending producer list to new consumer")

		bytesWriten, err := connection.Write([]byte(producerList))

		if err != nil {

			log.Print("Error sending list: ", err)
		}

		log.Printf("Sent %v bytes, waiting on response", bytesWriten)

		byteCount, err = connection.Read(data)

		if err != nil {

			log.Print("ERROR on connection read:", err)

			return
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

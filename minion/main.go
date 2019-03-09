package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"strings"

	"github.com/gorilla/mux"
)

func main() {

	httpRouter := mux.NewRouter()

	httpRouter.HandleFunc("/start", handleStart).Methods("POST")
	httpRouter.HandleFunc("/stop", handleStop).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":9090", httpRouter))

}

func handleStart(responseWriter http.ResponseWriter, httpRequest *http.Request) {
	body, err := ioutil.ReadAll(httpRequest.Body)

	if err != nil {

		log.Printf("Error reading body: %v", err)

		http.Error(responseWriter, "can't read body", http.StatusBadRequest)

		return
	}

	lines := strings.Fields(string(body))

	command := exec.Command(lines[0], lines[1:]...)

	command.Start()
}

func handleStop(responseWriter http.ResponseWriter, httpRequest *http.Request) {

	body, err := ioutil.ReadAll(httpRequest.Body)

	if err != nil {

		log.Printf("Error reading body: %v", err)

		http.Error(responseWriter, "can't read body", http.StatusBadRequest)

		return
	}

	lines := strings.Fields(string(body))

	command := exec.Command(lines[0], lines[1:]...)

	command.Start()
}

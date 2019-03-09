package main

import (
	"os"
	"os/exec"
	"time"
)

func main() {

	command := exec.Command("docker", "run", "hello-world")
	command.Start()
	time.Sleep(2 * time.Second)
	os.Exit(0)
}

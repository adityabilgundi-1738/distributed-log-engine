package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"distributed-log-engine/internal/mapreduce"
)

func main() {
	coordFlag := flag.String("coordinator", "127.0.0.1:9090", "Coordinator HTTP listen address")
	workersFlag := flag.String(
		"workers",
		"127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
		"Comma-separated list of HTTP Worker node listen addresses",
	)
	flag.Parse()

	workerAddrs := strings.Split(*workersFlag, ",")
	cleanup, err := mapreduce.StartEmbeddedCluster(*coordFlag, workerAddrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start Go MapReduce HTTP cluster: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	fmt.Printf("[Go MapReduce HTTP Coordinator] Listening on http://%s\n", *coordFlag)
	for i, w := range workerAddrs {
		fmt.Printf("  -> [HTTP Worker Node %d] Listening on http://%s\n", i+1, w)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}

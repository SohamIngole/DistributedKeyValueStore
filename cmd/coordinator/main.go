package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"context"
	"strings"
	"syscall"

	"DistributedKeyValueStore/internal/coordinator"
)

func main() {
	addr  := flag.String("addr", ":7000", "coordinator listen address")
	nodes := flag.String("nodes", "", "comma-separated list of backend node addresses")
	flag.Parse()

	if *nodes == "" {
		log.Fatal("at least one node address required via -nodes flag")
	}

	c := coordinator.New(*addr)

	// Register each backend node
	for _, nodeAddr := range strings.Split(*nodes, ",") {
		nodeAddr = strings.TrimSpace(nodeAddr)
		if nodeAddr != "" {
			c.AddNode(nodeAddr)
			log.Printf("registered node: %s", nodeAddr)
		}
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("shutting down coordinator...")
		cancel()
	}()

	if err := c.ListenAndServe(ctx); err != nil {
		log.Fatal(err)
	}
}
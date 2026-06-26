package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"rediska/config"
	"rediska/server"
	"syscall"
)

func setUpFlags() {
	flag.IntVar(&config.Port, "port", 7379, "Port for rediska")
	flag.StringVar(&config.Host, "host", "0.0.0.0", "Host ip address for rediska server")
	flag.BoolVar(&config.AppendOnly, "appendOnly", false, "Run rediska in appendOnly File mode")
	flag.StringVar(&config.AppendOnlyFile, "appendOnlyFile", "", "File name for storing SET commands in append only mode")
	flag.IntVar(&config.MaxMemory, "maxmemory", 0, "Maximum memory in megabytes for this server")
	flag.StringVar(&config.MaxMemoryPolicy, "maxmemory-policy", "noeviction", "Eviction policy to trigger")
	flag.IntVar(&config.MaxMemorySamples, "maxmemory-samples", 5, "Samples to use for eviction policy")
	flag.Parse()
}
func main() {
	setUpFlags()
	context, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("Starting Rediska on host %v and port %v  and append file %v\n", config.Host, config.Port, config.AppendOnlyFile)
	if err := server.Run(context, config.Host, config.Port, config.AppendOnly, config.AppendOnlyFile); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

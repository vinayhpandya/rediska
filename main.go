package main

import (
	"flag"
	"fmt"
	"log"
	"rediska/config"
	"rediska/server"
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
	fmt.Printf("Starting Rediska on host %v and port %v  and append file %v\n", config.Host, config.Port, config.AppendOnlyFile)
	if err := server.Run(config.Host, config.Port, config.AppendOnly, config.AppendOnlyFile); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

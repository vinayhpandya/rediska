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
	flag.Parse()
}
func main() {
	setUpFlags()
	fmt.Printf("Starting Rediska on host %v and port %v \n", config.Host, config.Port)
	if err := server.Run(config.Host, config.Port); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/config"
	"github.com/vinayhpandya/rediska/server"
	"github.com/vinayhpandya/rediska/store"
	"log"
	"net/http"
	_ "net/http/pprof" // <-- ADDED: registers /debug/pprof/* handlers
	"os"
	"os/signal"
	"syscall"
)

var pprofAddr string // <-- ADDED

func setUpFlags() {
	flag.IntVar(&config.Port, "port", 7379, "Port for rediska")
	flag.StringVar(&config.Host, "host", "0.0.0.0", "Host ip address for rediska server")
	flag.BoolVar(&config.AppendOnly, "appendOnly", false, "Run rediska in appendOnly File mode")
	flag.StringVar(&config.AppendOnlyFile, "appendOnlyFile", "", "File name for storing SET commands in append only mode")
	flag.IntVar(&config.MaxMemory, "maxmemory", 0, "Maximum memory in megabytes for this server")
	flag.StringVar(&config.MaxMemoryPolicy, "maxmemory-policy", "noeviction", "Eviction policy to trigger")
	flag.IntVar(&config.MaxMemorySamples, "maxmemory-samples", 5, "Samples to use for eviction policy")
	flag.IntVar(&config.LFULogFactor, "lfu-log-factor", 10, "Controls how quickly the LFU counter grows (higher = slower growth)")
	flag.IntVar(&config.LFUDecayTime, "lfu-decay-time", 1, "Minutes of idle time per LFU counter decay step")
	flag.BoolVar(&config.ClusterEnabled, "cluster-enabled", false, "Run rediska as a cluster node")
	// ADDED: opt-in flag, off by default (empty string = pprof never starts)
	flag.StringVar(&config.ClusterConfigFile, "cluster-config-file", "", "Cluster layout file (needed with --cluster-enabled)")
	flag.StringVar(&pprofAddr, "pprof-addr", "", "If set, expose net/http/pprof on this address (e.g. localhost:6060). Disabled by default.")
	flag.Parse()
}

func main() {
	setUpFlags()
	if config.ClusterEnabled {
		if config.ClusterConfigFile == "" {
			log.Fatal("--cluster-enabled needs --cluster-config-file")
		}
		layout, err := cluster.LoadLayout(config.ClusterConfigFile, config.Port)
		if err != nil {
			log.Fatal(err)
		}
		cluster.Current = layout
		log.Printf("cluster: I am %s (%s), owning slots %v", layout.Self.Name, layout.Self.Addr, layout.Self.Slots)
	}
	store.Default.SetPolicy(config.MaxMemoryPolicy)
	store.SetLFUParams(config.LFULogFactor, config.LFUDecayTime)
	context, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ADDED: start the pprof HTTP server on its own port, only if requested
	if pprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	fmt.Printf("Starting Rediska on host %v and port %v  and append file %v\n", config.Host, config.Port, config.AppendOnlyFile)
	if err := server.Run(context, config.Host, config.Port, config.AppendOnly, config.AppendOnlyFile); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

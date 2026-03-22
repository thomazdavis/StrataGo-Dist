package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/thomazdavis/stratago-dist/consensus"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"github.com/thomazdavis/stratago-dist/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Parse Command Line Flags (Mapped to your launch.json)
	nodeID := flag.String("node-id", "node1", "Unique ID for this node")
	raftPort := flag.Int("raft-port", 7001, "Port for Raft consensus protocol")
	grpcPort := flag.Int("grpc-port", 8001, "Port for gRPC client connections")
	raftHost := flag.String("raft-host", "127.0.0.1", "Hostname/IP that other nodes use to reach this node")
	joinAddr := flag.String("join-addr", "", "Raft address of the leader to join")
	flag.Parse()

	// Setup the data directory and
	// initialize the storage enginer for this specific node
	dataDir := filepath.Join("data", *nodeID)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	db, err := engine.Open(filepath.Join(dataDir, "engine"))
	if err != nil {
		log.Fatalf("Failed to start StrataGo engine: %v", err)
	}

	// Initialize the Consensus FSM (The Bridge)
	fsm := consensus.NewStrataFSM(db)

	// Configure Raft Storage (Logs, Stable Info, and Snapshots)
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		log.Fatalf("Failed to create Raft log store: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.bolt"))
	if err != nil {
		log.Fatalf("Failed to create Raft stable store: %v", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 1, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create Raft snapshot store: %v", err)
	}

	// Raft Networking
	// raftAddr := fmt.Sprintf("127.0.0.1:%d", *raftPort)
	bindAddr := fmt.Sprintf("0.0.0.0:%d", *raftPort)
	advertiseAddr := fmt.Sprintf("%s:%d", *raftHost, *raftPort)

	tcpAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		log.Fatalf("Failed to resolve Raft TCP address: %v", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create Raft TCP transport: %v", err)
	}

	// Boot the Raft Node
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}

	// Hidden HTTP server for automated cluster joining; Management API
	httpPort := *raftPort + 2000

	// status endpoint for Docker/Kubernetes healthchecks
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		addr := r.URL.Query().Get("addr")

		if raftNode.State() != raft.Leader {
			http.Error(w, "I am not the leader", http.StatusBadRequest)
			return
		}

		if err := raftNode.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0).Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	go http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil)

	// Cluster Bootstrapping Logic
	if *joinAddr == "" {
		// Seed node bootstraps the cluster
		configuration := raft.Configuration{
			Servers: []raft.Server{{ID: config.LocalID, Address: raft.ServerAddress(advertiseAddr)}},
		}
		raftNode.BootstrapCluster(configuration)
		fmt.Printf("[%s] Bootstrapped new Raft cluster.\n", *nodeID)
	} else {
		// Join an existing cluster via the Leader's HTTP Management API
		host, portStr, err := net.SplitHostPort(*joinAddr)
		if err != nil {
			log.Fatalf("Invalid join address format. Expected host:port, got '%s': %v", *joinAddr, err)
		}

		leaderRaftPort, err := strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("Invalid port in join address '%s': %v", *joinAddr, err)
		}

		leaderHttpPort := leaderRaftPort + 2000

		joinURL := fmt.Sprintf("http://%s:%d/join?id=%s&addr=%s", host, leaderHttpPort, *nodeID, advertiseAddr)

		go func() {
			for {
				resp, err := http.Get(joinURL)
				if err == nil && resp.StatusCode == http.StatusOK {
					fmt.Printf("[%s] Successfully joined the cluster!\n", *nodeID)
					break
				}
				time.Sleep(2 * time.Second) // Retry until the leader is fully booted
			}
		}()
	}

	// Start the gRPC Server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvServer := server.NewKVServer(raftNode, db)
	pb.RegisterKVStoreServer(grpcServer, kvServer)

	reflection.Register(grpcServer)

	fmt.Printf("\n Node '%s' is LIVE\n", *nodeID)
	fmt.Printf("   Raft Port: %d | gRPC Port: %d | Admin Port: %d\n\n", *raftPort, *grpcPort, httpPort)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC Server crashed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit // Block main thread here until signal is received

	fmt.Printf("\n[%s] Shutting down server...\n", *nodeID)

	// Stop accepting new public network traffic
	grpcServer.GracefulStop()

	// Close cached proxy connections
	kvServer.Close()

	// Safely step down from Raft leadership and close internal network
	if future := raftNode.Shutdown(); future.Error() != nil {
		fmt.Printf("Error shutting down Raft: %v\n", future.Error())
	}

	// Flush the active Memtable to disk and safely close the storage engine
	if err := db.Close(); err != nil {
		fmt.Printf("Error closing database: %v\n", err)
	}

	fmt.Printf("[%s] Server safely stopped.\n", *nodeID)
}

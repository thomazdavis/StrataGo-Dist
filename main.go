package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/thomazdavis/stratago-dist/consensus"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"github.com/thomazdavis/stratago-dist/server"
	"google.golang.org/grpc"
)

func main() {
	// Parse Command Line Flags (Mapped to your launch.json)
	nodeID := flag.String("node-id", "node1", "Unique ID for this node")
	raftPort := flag.Int("raft-port", 7001, "Port for Raft consensus protocol")
	grpcPort := flag.Int("grpc-port", 8001, "Port for gRPC client connections")
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
	logStore, _ := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	stableStore, _ := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.bolt"))
	snapshotStore, _ := raft.NewFileSnapshotStore(dataDir, 1, os.Stderr)

	// Raft Networking
	raftAddr := fmt.Sprintf("127.0.0.1:%d", *raftPort)
	tcpAddr, _ := net.ResolveTCPAddr("tcp", raftAddr)
	transport, _ := raft.NewTCPTransport(raftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)

	// Boot the Raft Node
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}

	// Hidden HTTP server for automated cluster joining; Management API
	httpPort := *raftPort + 2000
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
			Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}},
		}
		raftNode.BootstrapCluster(configuration)
		fmt.Printf("[%s] Bootstrapped new Raft cluster.\n", *nodeID)
	} else {
		// Join an existing cluster via the Leader's HTTP Management API
		host, portStr, _ := net.SplitHostPort(*joinAddr)
		leaderRaftPort, _ := strconv.Atoi(portStr)
		leaderHttpPort := leaderRaftPort + 2000

		joinURL := fmt.Sprintf("http://%s:%d/join?id=%s&addr=%s", host, leaderHttpPort, *nodeID, raftAddr)

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

	fmt.Printf("\n Node '%s' is LIVE\n", *nodeID)
	fmt.Printf("   Raft Port: %d | gRPC Port: %d | Admin Port: %d\n\n", *raftPort, *grpcPort, httpPort)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC Server crashed: %v", err)
	}
}

package server

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/thomazdavis/stratago-dist/consensus"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGetLeaderGRPCAddress(t *testing.T) {
	addr, err := getLeaderGRPCAddress("127.0.0.1:7001")
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8001", addr)

	_, err = getLeaderGRPCAddress("")
	assert.Error(t, err, "Should fail if no leader is elected")
	assert.Equal(t, "no leader currently elected", err.Error())

	_, err = getLeaderGRPCAddress("invalid-addr-format")
	assert.Error(t, err)
}

// setupSingleNodeCluster builds a real gRPC server + Raft leader for end-to-end network testing
func setupSingleNodeCluster(t *testing.T) (*KVServer, pb.KVStoreClient, func()) {
	// Setup ephemeral Storage Engine
	dir, _ := os.MkdirTemp("", "server_test")
	db, _ := engine.Open(dir)

	// Setup In-Memory Raft Node (No disk I/O for instant test execution)
	fsm := consensus.NewStrataFSM(db)
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()
	_, transport := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:7000"))

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("node0")
	// Drop timeouts drastically so tests don't wait 3 seconds for an election
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 20 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond

	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		t.Fatalf("Failed to initialize raft: %v", err)
	}

	// Force this single node to become the leader
	raftNode.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}},
	})

	// Wait exactly long enough for the fast election to resolve
	time.Sleep(200 * time.Millisecond)

	// Bind a real gRPC Server to a random available port (:0)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to bind gRPC port: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvServer := NewKVServer(raftNode, db)
	pb.RegisterKVStoreServer(grpcServer, kvServer)

	go grpcServer.Serve(lis)

	// Setup a real gRPC Client to act as the user
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial gRPC server: %v", err)
	}
	client := pb.NewKVStoreClient(conn)

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		raftNode.Shutdown()
		db.Close()
		os.RemoveAll(dir)
	}

	return kvServer, client, cleanup
}

func TestKVServer_PutAndGet(t *testing.T) {
	_, client, cleanup := setupSingleNodeCluster(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test Network PUT
	putResp, err := client.Put(ctx, &pb.PutRequest{
		Key:   "network_key",
		Value: []byte("network_value"),
	})
	assert.NoError(t, err)
	assert.True(t, putResp.Success, "Put operation failed on server")

	// Test Network GET (Strong Consistency via Raft)
	getResp, err := client.Get(ctx, &pb.GetRequest{
		Key:         "network_key",
		Consistency: pb.GetRequest_STRONG,
	})
	assert.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("network_value"), getResp.Value)

	// Test Network GET (Missing Key)
	getRespMissing, err := client.Get(ctx, &pb.GetRequest{
		Key:         "void",
		Consistency: pb.GetRequest_FAST,
	})
	assert.NoError(t, err)
	assert.False(t, getRespMissing.Found)
}

func TestKVServer_ConnectionCaching(t *testing.T) {
	kvServer, _, cleanup := setupSingleNodeCluster(t)
	defer cleanup()

	// Initial Call: Should create a new cached connection
	// Even though this node is the leader, we can manually trigger the client fetcher
	client1, err := kvServer.getLeaderClient()
	assert.NoError(t, err)
	assert.NotNil(t, client1)

	conn1 := kvServer.leaderConn
	assert.NotNil(t, conn1, "leaderConn must be cached")

	// Second Call: Should reuse the exact same TCP connection
	client2, err := kvServer.getLeaderClient()
	assert.NoError(t, err)
	assert.NotNil(t, client2)

	conn2 := kvServer.leaderConn
	assert.Equal(t, conn1, conn2, "FATAL: Connection was not reused. TCP churn detected.")

	// Simulate a Raft Leader Election (Topology Change)
	// We manually manipulate the struct state to trick the caching logic
	kvServer.mu.Lock()
	kvServer.leaderAddr = "127.0.0.1:9999" // Fake a new leader address
	kvServer.mu.Unlock()

	// Third Call: Should purge the old connection and dial the new one
	_, err = kvServer.getLeaderClient()
	assert.NoError(t, err)

	conn3 := kvServer.leaderConn
	assert.NotEqual(t, conn1, conn3, "Stale connection was not purged after leader change")
}

// BenchmarkReadConsistency tests the latency and throughput difference
// between quorum reads and lease-based local reads
func BenchmarkReadConsistency(b *testing.B) {
	_, client, cleanup := setupSingleNodeCluster(&testing.T{})
	defer cleanup()

	ctx := context.Background()
	testKey := "bench_key"
	testVal := []byte("bench_value_data")

	_, err := client.Put(ctx, &pb.PutRequest{Key: testKey, Value: testVal})
	if err != nil {
		b.Fatalf("Failed to seed database: %v", err)
	}

	// Benchmark STRONG Consistency (Quorum/Barrier)
	b.Run("STRONG_Consistency", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				resp, err := client.Get(ctx, &pb.GetRequest{
					Key:         testKey,
					Consistency: pb.GetRequest_STRONG,
				})
				if err != nil || !resp.Found {
					b.Error("STRONG read failed")
				}
			}
		})
	})

	// Benchmark FAST Consistency (Leader Lease)
	b.Run("FAST_Consistency", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				resp, err := client.Get(ctx, &pb.GetRequest{
					Key:         testKey,
					Consistency: pb.GetRequest_FAST,
				})
				if err != nil || !resp.Found {
					b.Error("FAST read failed")
				}
			}
		})
	})
}

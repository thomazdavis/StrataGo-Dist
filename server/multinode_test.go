package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
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

type testNode struct {
	id         string
	raftAddr   string
	grpcAddr   string
	dir        string
	db         *engine.StrataGo
	raft       *raft.Raft
	grpcServer *grpc.Server
	client     pb.KVStoreClient
	conn       *grpc.ClientConn
}

func setupClusterNode(t testing.TB, id, raftPort, grpcPort string) *testNode {
	dir, _ := os.MkdirTemp("", "cluster_test_"+id)
	db, _ := engine.Open(dir)

	raftAddr := fmt.Sprintf("127.0.0.1:%s", raftPort)
	grpcAddr := fmt.Sprintf("127.0.0.1:%s", grpcPort)

	// Setup TCP Transport for Raft
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	assert.NoError(t, err)
	transport, err := raft.NewTCPTransport(raftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	assert.NoError(t, err)

	fsm := consensus.NewStrataFSM(db)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	config.HeartbeatTimeout = 100 * time.Millisecond
	config.ElectionTimeout = 100 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond

	raftNode, err := raft.NewRaft(config, fsm, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), transport)
	assert.NoError(t, err)

	// Setup gRPC Server
	lis, err := net.Listen("tcp", grpcAddr)
	assert.NoError(t, err)
	grpcServer := grpc.NewServer()
	kvServer := NewKVServer(raftNode, db)
	pb.RegisterKVStoreServer(grpcServer, kvServer)
	go grpcServer.Serve(lis)

	// Setup gRPC Client
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	client := pb.NewKVStoreClient(conn)

	return &testNode{
		id: id, raftAddr: raftAddr, grpcAddr: grpcAddr, dir: dir,
		db: db, raft: raftNode, grpcServer: grpcServer, client: client, conn: conn,
	}
}

// setupRunningCluster initializes a 3-node cluster, bootstraps it, waits for an election,
// and returns the leader, a follower, and a unified cleanup function.
func setupRunningCluster(t testing.TB, idPrefix string, baseRaftPort, baseGrpcPort int) (*testNode, *testNode, func()) {
	node1 := setupClusterNode(t, fmt.Sprintf("%s_1", idPrefix), fmt.Sprintf("%d", baseRaftPort+1), fmt.Sprintf("%d", baseGrpcPort+1))
	node2 := setupClusterNode(t, fmt.Sprintf("%s_2", idPrefix), fmt.Sprintf("%d", baseRaftPort+2), fmt.Sprintf("%d", baseGrpcPort+2))
	node3 := setupClusterNode(t, fmt.Sprintf("%s_3", idPrefix), fmt.Sprintf("%d", baseRaftPort+3), fmt.Sprintf("%d", baseGrpcPort+3))

	cleanup := func() {
		for _, n := range []*testNode{node1, node2, node3} {
			n.conn.Close()
			n.grpcServer.Stop()
			n.raft.Shutdown()
			n.db.Close()
			os.RemoveAll(n.dir)
		}
	}

	// Bootstrap the cluster together
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: raft.ServerID(node1.id), Address: raft.ServerAddress(node1.raftAddr)},
			{ID: raft.ServerID(node2.id), Address: raft.ServerAddress(node2.raftAddr)},
			{ID: raft.ServerID(node3.id), Address: raft.ServerAddress(node3.raftAddr)},
		},
	}
	node1.raft.BootstrapCluster(configuration)

	// Wait for an election to settle and find the Leader vs Follower
	time.Sleep(2 * time.Second)

	var leader, follower *testNode
	for _, n := range []*testNode{node1, node2, node3} {
		if n.raft.State() == raft.Leader {
			leader = n
		} else {
			follower = n
		}
	}

	if leader == nil || follower == nil {
		t.Fatalf("Cluster failed to elect a leader within the timeout")
	}

	return leader, follower, cleanup
}

func TestMultiNodeCluster_ProxyAndReplication(t *testing.T) {
	leader, follower, cleanup := setupRunningCluster(t, "node", 17000, 18000)
	defer cleanup()

	// Send a write to the Follower
	// This forces the Follower to intercept the gRPC call, identify the Leader's Raft port,
	// calculate the Leader's gRPC port (+1000), dial it, and proxy the Protobuf payload
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	putResp, err := follower.client.Put(ctx, &pb.PutRequest{
		Key:   "proxy_test_key",
		Value: []byte("replicated_value"),
	})

	assert.NoError(t, err, "Follower failed to proxy the write request")
	assert.True(t, putResp.Success)

	// Verify Replication on a completely different node via STRONG consistency
	// We read from the Leader to ensure the Follower's proxied write actually committed to the Raft log.
	getResp, err := leader.client.Get(ctx, &pb.GetRequest{
		Key:         "proxy_test_key",
		Consistency: pb.GetRequest_STRONG,
	})

	assert.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("replicated_value"), getResp.Value, "Data did not replicate through the consensus engine")
}

// BenchmarkMultiNode_WriteThroughput measures the overhead of the gRPC proxy layer
func BenchmarkMultiNode_WriteThroughput(b *testing.B) {
	leader, follower, cleanup := setupRunningCluster(b, "bench_node", 17010, 18010)
	defer cleanup()

	ctx := context.Background()
	var counter uint64

	// Benchmark 1: Direct Leader Writes (The Baseline)
	// Client -> Leader -> Raft Quorum -> Disk
	b.Run("Direct_Leader_Write", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				idx := atomic.AddUint64(&counter, 1)
				key := fmt.Sprintf("direct_key_%d", idx)
				_, err := leader.client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte("val")})
				if err != nil {
					b.Error(err)
				}
			}
		})
	})

	// Benchmark 2: Proxied Follower Writes (The Proxy Overhead)
	// Client -> Follower -> gRPC Proxy -> Leader -> Raft Quorum -> Disk
	b.Run("Proxied_Follower_Write", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				idx := atomic.AddUint64(&counter, 1)
				key := fmt.Sprintf("proxy_key_%d", idx)
				_, err := follower.client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte("val")})
				if err != nil {
					b.Error(err)
				}
			}
		})
	})
}

// BenchmarkMultiNode_ReadConsistency measures the true network impact of CAP theorem tradeoffs
func BenchmarkMultiNode_ReadConsistency(b *testing.B) {
	leader, follower, cleanup := setupRunningCluster(b, "read_node", 17020, 18020)
	defer cleanup()

	ctx := context.Background()
	testKey := "consistency_key"
	testVal := []byte("consistency_value")

	// Seed the database via the Leader
	_, err := leader.client.Put(ctx, &pb.PutRequest{Key: testKey, Value: testVal})
	if err != nil {
		b.Fatalf("Failed to seed data: %v", err)
	}

	// Give Followers a moment to apply the replicated log to their local disk
	time.Sleep(500 * time.Millisecond)

	// Strong Consistency (CP)
	// Requires Leader to ping Followers over TCP to ensure no split-brain
	b.Run("1_STRONG_Leader_Quorum", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				_, err := leader.client.Get(ctx, &pb.GetRequest{
					Key:         testKey,
					Consistency: pb.GetRequest_STRONG,
				})
				if err != nil {
					b.Error(err)
				}
			}
		})
	})

	// Fast Consistency (AP - Leader)
	// Leader bypasses network, trusts its local lease timer
	b.Run("2_FAST_Leader_Lease", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				_, err := leader.client.Get(ctx, &pb.GetRequest{
					Key:         testKey,
					Consistency: pb.GetRequest_FAST,
				})
				if err != nil {
					b.Error(err)
				}
			}
		})
	})

	// Eventual Consistency (AP - Follower)
	// Client reads directly from a Follower's local disk. No proxying, no leases
	// Maximum throughput, but data might be slightly stale
	b.Run("3_EVENTUAL_Follower_Local", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				_, err := follower.client.Get(ctx, &pb.GetRequest{
					Key:         testKey,
					Consistency: pb.GetRequest_EVENTUAL,
				})
				if err != nil {
					b.Error(err)
				}
			}
		})
	})
}

func TestMultiNodeCluster_LeaderCrashRecovery(t *testing.T) {
	node1 := setupClusterNode(t, "chaos1", "17031", "18031")
	node2 := setupClusterNode(t, "chaos2", "17032", "18032")
	node3 := setupClusterNode(t, "chaos3", "17033", "18033")
	nodes := []*testNode{node1, node2, node3}

	defer func() {
		for _, n := range nodes {
			n.conn.Close()
			n.grpcServer.Stop()
			if n.raft != nil {
				n.raft.Shutdown()
			}
			n.db.Close()
			os.RemoveAll(n.dir)
		}
	}()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: raft.ServerID(node1.id), Address: raft.ServerAddress(node1.raftAddr)},
			{ID: raft.ServerID(node2.id), Address: raft.ServerAddress(node2.raftAddr)},
			{ID: raft.ServerID(node3.id), Address: raft.ServerAddress(node3.raftAddr)},
		},
	}
	node1.raft.BootstrapCluster(configuration)
	time.Sleep(2 * time.Second)

	var initialLeader *testNode
	for _, n := range nodes {
		if n.raft.State() == raft.Leader {
			initialLeader = n
			break
		}
	}
	if initialLeader == nil {
		t.Fatalf("FATAL: Failed to elect initial leader")
	}

	// Write the data before the crash
	ctxPre, cancelPre := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelPre()
	_, err := initialLeader.client.Put(ctxPre, &pb.PutRequest{
		Key:   "pre_crash_key",
		Value: []byte("critical_data"),
	})
	assert.NoError(t, err, "Failed to write initial data")

	// Fire a concurrent write exactly as the leader dies
	inflightDone := make(chan error, 1)
	go func() {
		ctxInflight, cancelInflight := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelInflight()
		_, err := initialLeader.client.Put(ctxInflight, &pb.PutRequest{
			Key:   "inflight_key",
			Value: []byte("maybe_lost"),
		})
		inflightDone <- err
	}()

	// Kill the Leader
	initialLeader.conn.Close()
	initialLeader.grpcServer.Stop()
	initialLeader.raft.Shutdown()
	initialLeader.db.Close()

	var survivors []*testNode
	for _, n := range nodes {
		if n.id != initialLeader.id {
			survivors = append(survivors, n)
		}
	}

	// Wait for Election Timeout + Buffer
	time.Sleep(1500 * time.Millisecond)

	var newLeader *testNode
	for _, n := range survivors {
		if n.raft.State() == raft.Leader {
			newLeader = n
			break
		}
	}
	if newLeader == nil {
		t.Fatalf("FATAL: Survivors failed to achieve quorum and elect a new leader")
	}

	// Verify pre-crash data survived
	ctxPost, cancelPost := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelPost()
	getResp, err := newLeader.client.Get(ctxPost, &pb.GetRequest{
		Key:         "pre_crash_key",
		Consistency: pb.GetRequest_STRONG,
	})
	assert.NoError(t, err)
	assert.True(t, getResp.Found, "Data loss detected: Pre-crash data did not replicate to the new leader")
	assert.Equal(t, []byte("critical_data"), getResp.Value)

	// Verify the in-flight write
	t.Logf("In-flight write client error (expected): %v", <-inflightDone)

	ctxInflightCheck, cancelInflightCheck := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelInflightCheck()
	inflightResp, err := newLeader.client.Get(ctxInflightCheck, &pb.GetRequest{
		Key:         "inflight_key",
		Consistency: pb.GetRequest_STRONG,
	})
	assert.NoError(t, err)

	if inflightResp.Found {
		t.Log("Result: The in-flight write hit quorum just before the leader died. Data preserved.")
		assert.Equal(t, []byte("maybe_lost"), inflightResp.Value, "FATAL: Torn write detected. Data was corrupted.")
	} else {
		t.Log("Result: The in-flight write failed to hit quorum. It was cleanly dropped.")
	}

	// Verify the cluster is fully healed and accepting new writes
	ctxNew, cancelNew := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelNew()
	putResp, err := newLeader.client.Put(ctxNew, &pb.PutRequest{
		Key:   "post_crash_key",
		Value: []byte("new_data"),
	})
	assert.NoError(t, err)
	assert.True(t, putResp.Success, "Cluster is locked in a read-only state and cannot process new writes")
}

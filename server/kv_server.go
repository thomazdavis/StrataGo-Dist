package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type KVServer struct {
	pb.UnimplementedKVStoreServer
	Raft       *raft.Raft
	DB         *engine.StrataGo
	mu         sync.RWMutex
	leaderConn *grpc.ClientConn
	leaderAddr string
}

func NewKVServer(r *raft.Raft, db *engine.StrataGo) *KVServer {
	return &KVServer{
		Raft: r,
		DB:   db,
	}
}

func getLeaderGRPCAddress(raftAddr string) (string, error) {
	if raftAddr == "" {
		return "", fmt.Errorf("no leader currently elected")
	}

	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", err
	}

	raftPort, err := strconv.Atoi(portStr)
	if err != nil {
		return "", err
	}

	// We offset our gRPC ports by exactly 1000
	grpcPort := raftPort + 1000
	return fmt.Sprintf("%s:%d", host, grpcPort), nil
}

// getLeaderClient manages the cached gRPC connection to the current leader
// It reuses the TCP connection unless the leader has changed
func (s *KVServer) getLeaderClient() (pb.KVStoreClient, error) {
	currentLeaderAddr := string(s.Raft.Leader())
	if currentLeaderAddr == "" {
		return nil, fmt.Errorf("no leader currently elected")
	}

	grpcAddr, err := getLeaderGRPCAddress(currentLeaderAddr)
	if err != nil {
		return nil, err
	}

	// check if we already have a valid connection
	s.mu.RLock()
	if s.leaderAddr == grpcAddr && s.leaderConn != nil {
		client := pb.NewKVStoreClient(s.leaderConn)
		s.mu.RUnlock()
		return client, nil
	}
	s.mu.RUnlock()

	// Write lock to establish a new connection
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leaderAddr == grpcAddr && s.leaderConn != nil {
		return pb.NewKVStoreClient(s.leaderConn), nil
	}

	// Close the stale connection if one exists
	if s.leaderConn != nil {
		s.leaderConn.Close()
	}

	// Dial the new leader
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader: %w", err)
	}

	// Cache the connection
	s.leaderConn = conn
	s.leaderAddr = grpcAddr

	return pb.NewKVStoreClient(conn), nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Reject writes if this node is not the leader
	if s.Raft.State() != raft.Leader {
		client, err := s.getLeaderClient()
		if err != nil {
			return &pb.PutResponse{Success: false, Message: err.Error()}, nil
		}
		// Act as a client and forward the exact request using the cached connection
		return client.Put(ctx, req)
	}

	// Event payload
	cmd := &pb.Command{
		Op:    pb.Command_PUT,
		Key:   req.Key,
		Value: req.Value,
	}

	eventBytes, err := proto.Marshal(cmd)
	if err != nil {
		return &pb.PutResponse{Success: false, Message: fmt.Sprintf("failed to marshal command: %v", err)}, nil
	}

	// Propose the write to the raft cluster
	applyFuture := s.Raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		// to check if we lost the leadership in-between
		if err == raft.ErrNotLeader {
			return &pb.PutResponse{Success: false, Message: "leadership lost during write, please retry"}, nil
		}
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// Wait for the fsm to apply the log and return the result
	response := applyFuture.Response()
	if err, ok := response.(error); ok && err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.PutResponse{Success: true, Message: "Key saved successfully"}, nil
}

// Get handles read requests with Tunable Consistency
func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	switch req.Consistency {
	case pb.GetRequest_STRONG:
		// Requires a network round-trip to verify leadership with the quorum
		if err := s.Raft.VerifyLeader().Error(); err != nil {
			leaderAddr := string(s.Raft.Leader())
			if leaderAddr == "" {
				leaderAddr = "unknown"
			}
			return nil, status.Errorf(codes.FailedPrecondition, "stale read prevented: not the leader (leader: %s)", leaderAddr)
		}

	case pb.GetRequest_FAST:
		// Zero network hops, fast but relies on bounded clock drift
		if s.Raft.State() != raft.Leader {
			leaderAddr := string(s.Raft.Leader())
			if leaderAddr == "" {
				leaderAddr = "unknown"
			}
			return nil, status.Errorf(codes.FailedPrecondition, "not the leader (leader: %s)", leaderAddr)
		}

	case pb.GetRequest_EVENTUAL:
		// Do nothing. Read from local engine regardless of Raft state.
	}

	// Fetch directly from the isolated storage engine
	val, found := s.DB.Get([]byte(req.Key))

	return &pb.GetResponse{
		Found:         found,
		Value:         val,
		LeaderAddress: string(s.Raft.Leader()),
	}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.Raft.State() != raft.Leader {
		client, err := s.getLeaderClient()
		if err != nil {
			return &pb.DeleteResponse{Success: false, LeaderAddress: string(s.Raft.Leader())}, err
		}
		return client.Delete(ctx, req)
	}

	cmd := &pb.Command{
		Op:  pb.Command_DELETE,
		Key: req.Key,
	}

	eventBytes, err := proto.Marshal(cmd)
	if err != nil {
		return &pb.DeleteResponse{Success: false, LeaderAddress: string(s.Raft.Leader())}, fmt.Errorf("failed to marshal command: %w", err)
	}

	applyFuture := s.Raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		if err == raft.ErrNotLeader {
			return &pb.DeleteResponse{Success: false, LeaderAddress: string(s.Raft.Leader())}, fmt.Errorf("leadership lost during write, please retry")
		}
		return &pb.DeleteResponse{Success: false}, nil
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// Close gracefully shuts down the cached gRPC connection to the leader
func (s *KVServer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaderConn != nil {
		s.leaderConn.Close()
	}
}

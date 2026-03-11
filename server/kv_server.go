package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
)

type KVServer struct {
	pb.UnimplementedKVStoreServer
	Raft *raft.Raft
	DB   *engine.StrataGo
}

func NewKVServer(r *raft.Raft, db *engine.StrataGo) *KVServer {
	return &KVServer{
		Raft: r,
		DB:   db,
	}
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Reject writes if this node is not the leader
	if s.Raft.State() != raft.Leader {
		return &pb.PutResponse{
			Success:       false,
			Message:       "NOT the leader",
			LeaderAddress: string(s.Raft.Leader()),
		}, nil
	}

	// Event payload
	event := map[string]interface{}{
		"op":    "put",
		"key":   req.Key,
		"value": req.Value,
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	// Propose the write to the raft cluster
	applyFuture := s.Raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
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
			return nil, fmt.Errorf("stale read prevented: not the leader")
		}

	case pb.GetRequest_FAST:
		// Zero network hops, fast but relies on bounded clock drift
		if s.Raft.State() != raft.Leader {
			return nil, fmt.Errorf("not the leader")
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
		return &pb.DeleteResponse{Success: false, LeaderAddress: string(s.Raft.Leader())}, nil
	}

	event := map[string]interface{}{
		"op":  "delete",
		"key": req.Key,
	}
	eventBytes, _ := json.Marshal(event)

	applyFuture := s.Raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return &pb.DeleteResponse{Success: false}, nil
	}

	return &pb.DeleteResponse{Success: true}, nil
}

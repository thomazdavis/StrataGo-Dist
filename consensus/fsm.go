package consensus

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	engine "github.com/thomazdavis/stratago-dist/engine"
)

type Event struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type StrataFSM struct {
	db *engine.StrataGo
}

func NewStrataFSM(db *engine.StrataGo) *StrataFSM {
	return &StrataFSM{db: db}
}

// Apply is invoked by Raft once a log entry is committed by a quorum of nodes
func (f *StrataFSM) Apply(l *raft.Log) interface{} {
	var e Event
	if err := json.Unmarshal(l.Data, &e); err != nil {
		return fmt.Errorf("failed to unmarshal Raft log: %w", err)
	}

	switch e.Op {
	case "put":
		return f.db.Put([]byte(e.Key), e.Value)
	case "delete":
		return f.db.Delete([]byte(e.Key))
	default:
		return fmt.Errorf("unknown operation: %s", e.Op)
	}
}

// Snapshot is called to support log compaction
func (f *StrataFSM) Snapshot() (raft.FSMSnapshot, error) {
	state, err := f.db.ScanAll()
	if err != nil {
		return nil, fmt.Errorf("failed to scan database for snapshot: %w", err)
	}

	return &strataSnapshot{state: state}, nil
}

// Restore is used when a node wakes up and needs to load a snapshot to catch up.
func (f *StrataFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Pass the network stream directly to the engine to wipe and reload
	if err := f.db.ClearAndLoad(rc); err != nil {
		return fmt.Errorf("failed to restore from snapshot: %w", err)
	}

	return nil
}

type strataSnapshot struct {
	state map[string][]byte
}

// Persist is called by a background Raft goroutine
// It encodes our map into a byte stream and writes it to the hard drive (snapshotStore)
func (s *strataSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode the in-memory map to JSON directly into the file sink
		encoder := json.NewEncoder(sink)
		if err := encoder.Encode(s.state); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel() // Tell Raft the snapshot failed so it doesn't delete the logs
		return err
	}

	return nil
}

// Release is called when Raft is done with the snapshot.
// We let the Go garbage collector handle the map, so we do nothing
func (s *strataSnapshot) Release() {}

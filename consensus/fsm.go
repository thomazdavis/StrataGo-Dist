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
	return &dummySnapshot{}, nil
}

// Restore is used when a node wakes up and needs to load a snapshot to catch up.
func (f *StrataFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return nil
}

type dummySnapshot struct{}

func (s *dummySnapshot) Persist(sink raft.SnapshotSink) error {
	sink.Write([]byte("dummy"))
	return sink.Close()
}

func (s *dummySnapshot) Release() {}

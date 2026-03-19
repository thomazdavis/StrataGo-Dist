package consensus

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"google.golang.org/protobuf/proto"
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
	var cmd pb.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal Raft command: %w", err)
	}

	switch cmd.Op {
	case pb.Command_PUT:
		return f.db.Put([]byte(cmd.Key), cmd.Value)
	case pb.Command_DELETE:
		return f.db.Delete([]byte(cmd.Key))
	default:
		return fmt.Errorf("unknown operation: %v", cmd.Op)
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

// Restore streams length-prefixed Protobufs to rebuild the database
func (f *StrataFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Wipe the current local state
	if err := f.db.Purge(); err != nil {
		return fmt.Errorf("failed to purge engine before restore: %w", err)
	}

	// Decode the length-prefixed binary stream
	for {
		// Read the 4-byte size header
		var length uint32
		if err := binary.Read(rc, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break // End of snapshot stream
			}
			return fmt.Errorf("failed to read snapshot length prefix: %w", err)
		}

		// Read the exact bytes for this specific KVEntry
		buf := make([]byte, length)
		if _, err := io.ReadFull(rc, buf); err != nil {
			return fmt.Errorf("failed to read snapshot payload: %w", err)
		}

		// Unmarshal and apply to the clean engine
		var entry pb.KVEntry
		if err := proto.Unmarshal(buf, &entry); err != nil {
			return fmt.Errorf("failed to unmarshal snapshot entry: %w", err)
		}

		if err := f.db.Put([]byte(entry.Key), entry.Value); err != nil {
			return fmt.Errorf("failed to ingest restored key %s: %w", entry.Key, err)
		}
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
		for k, v := range s.state {
			entry := &pb.KVEntry{
				Key:   k,
				Value: v,
			}
			data, err := proto.Marshal(entry)
			if err != nil {
				return err
			}

			// Write 4-byte size header
			if err := binary.Write(sink, binary.LittleEndian, uint32(len(data))); err != nil {
				return err
			}
			// Write the Protobuf payload
			if _, err := sink.Write(data); err != nil {
				return err
			}
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

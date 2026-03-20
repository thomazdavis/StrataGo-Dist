package consensus

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	engine "github.com/thomazdavis/stratago-dist/engine"
	pb "github.com/thomazdavis/stratago-dist/proto/gen"
	"google.golang.org/protobuf/proto"
)

// mockSnapshotSink implements raft.SnapshotSink for purely in-memory testing
type mockSnapshotSink struct {
	bytes.Buffer
	canceled bool
	closed   bool
}

// setupEngine provides a temporary, clean storage engine for testing
func setupEngine(t *testing.T) (*engine.StrataGo, string) {
	dir, err := os.MkdirTemp("", "fsm_test_dir")
	assert.NoError(t, err)

	db, err := engine.Open(dir)
	assert.NoError(t, err)

	return db, dir
}

func TestFSM_Apply(t *testing.T) {
	db, dir := setupEngine(t)
	defer os.RemoveAll(dir)
	defer db.Close()

	fsm := NewStrataFSM(db)

	// Test PUT Command
	putCmd := &pb.Command{
		Op:    pb.Command_PUT,
		Key:   "user_1",
		Value: []byte("davis"),
	}
	putBytes, _ := proto.Marshal(putCmd)

	// Simulate Raft committing a log
	fsm.Apply(&raft.Log{Data: putBytes})

	val, found := db.Get([]byte("user_1"))
	assert.True(t, found, "Apply(PUT) failed to write to database")
	assert.Equal(t, []byte("davis"), val)

	// Test DELETE Command
	delCmd := &pb.Command{
		Op:  pb.Command_DELETE,
		Key: "user_1",
	}
	delBytes, _ := proto.Marshal(delCmd)

	fsm.Apply(&raft.Log{Data: delBytes})

	_, found = db.Get([]byte("user_1"))
	assert.False(t, found, "Apply(DELETE) failed to remove key")
}

func (m *mockSnapshotSink) ID() string    { return "mock_sink" }
func (m *mockSnapshotSink) Cancel() error { m.canceled = true; return nil }
func (m *mockSnapshotSink) Close() error  { m.closed = true; return nil }

func TestFSM_SnapshotAndRestore(t *testing.T) {
	// Setup Node A (The Leader)
	dbA, dirA := setupEngine(t)
	defer os.RemoveAll(dirA)
	defer dbA.Close()

	fsmA := NewStrataFSM(dbA)

	// Pre-load Node A with data
	dbA.Put([]byte("key_A"), []byte("val_A"))
	dbA.Put([]byte("key_B"), []byte("val_B"))

	// Capture Snapshot from Node A
	snapshot, err := fsmA.Snapshot()
	assert.NoError(t, err, "Snapshot() failed")

	// Persist to our in-memory network stream
	sink := &mockSnapshotSink{}
	err = snapshot.Persist(sink)
	assert.NoError(t, err, "Persist() failed")
	assert.True(t, sink.closed, "Persist() did not close the sink upon success")

	// Setup Node B (The Follower)
	dbB, dirB := setupEngine(t)
	defer os.RemoveAll(dirB)
	defer dbB.Close()

	fsmB := NewStrataFSM(dbB)

	// Add trash data to Node B to prove Restore purges it correctly
	dbB.Put([]byte("trash_key"), []byte("trash_val"))

	// Restore Node B using Node A's snapshot data stream
	err = fsmB.Restore(io.NopCloser(&sink.Buffer))
	assert.NoError(t, err, "Restore() failed")

	// Verify Node B exactly matches Node A
	_, foundTrash := dbB.Get([]byte("trash_key"))
	assert.False(t, foundTrash, "Restore() failed to purge old database state")

	valA, foundA := dbB.Get([]byte("key_A"))
	assert.True(t, foundA)
	assert.Equal(t, []byte("val_A"), valA)

	valB, foundB := dbB.Get([]byte("key_B"))
	assert.True(t, foundB)
	assert.Equal(t, []byte("val_B"), valB)
}

func TestFSM_Apply_CorruptData(t *testing.T) {
	db, dir := setupEngine(t)
	defer os.RemoveAll(dir)
	defer db.Close()

	fsm := NewStrataFSM(db)

	// Feed raw garbage bytes instead of a Protobuf
	result1 := fsm.Apply(&raft.Log{Data: []byte("this is completely invalid garbage data")})

	err1, ok := result1.(error)
	assert.True(t, ok, "Apply should return an error interface for bad data")
	assert.Contains(t, err1.Error(), "failed to unmarshal", "Did not catch protobuf error")

	// Valid Protobuf, but an unknown operation code
	badCmd := &pb.Command{
		Op:  pb.Command_Operation(999), // Invalid Enum
		Key: "ghost",
	}
	badBytes, _ := proto.Marshal(badCmd)

	result2 := fsm.Apply(&raft.Log{Data: badBytes})

	err2, ok := result2.(error)
	assert.True(t, ok, "Apply should return an error interface for unknown operations")
	assert.Contains(t, err2.Error(), "unknown operation", "Did not catch invalid Op code")
}

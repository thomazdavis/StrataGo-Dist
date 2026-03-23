package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NodeID is set globally on boot by main.go
var NodeID string

var (
	RaftState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stratago_raft_state",
		Help: "Current Raft state (0=Follower, 1=Candidate, 2=Leader)",
	}, []string{"node_id"})

	ElectionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stratago_raft_elections_total",
		Help: "Total number of Raft elections initiated",
	}, []string{"node_id"})

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stratago_grpc_request_duration_seconds",
		Help:    "gRPC request latencies in seconds",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"node_id", "method", "consistency"})

	MemtableSizeBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stratago_memtable_size_bytes",
		Help: "Current byte size of the active Memtable",
	}, []string{"node_id"})

	SSTableCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stratago_sstable_count",
		Help: "Total number of SSTables on disk",
	}, []string{"node_id"})

	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stratago_requests_total",
		Help: "Total operations processed",
	}, []string{"node_id", "method"})
)

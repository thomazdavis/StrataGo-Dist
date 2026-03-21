# StrataGo Distributed Database

This document outlines the architectural design, empirical validation, and benchmarks established for the StrataGo distributed storage system.

## Core Architecture

StrataGo is a distributed key-value database that pairs a custom Log-Structured Merge Tree storage engine with a robust Raft consensus network layer to achieve high availability and fault tolerance.

## Storage Engine Layer

- Log-Structured Merge Tree: Built from scratch in Go to handle high-throughput operations.
- Internal Components: Features a Write-Ahead Log for crash recovery, in-memory Memtables for fast buffering, on-disk SSTables for immutable storage, and Bloom Filters for optimized read paths.

## Consensus and Replication Layer

- Raft Integration: Implemented a finite state machine that acts as a strict boundary between the network consensus layer and the physical disk. It correctly handles log application, snapshot generation, and state restoration for cluster synchronization.
- Poison Pill Handling: The finite state machine gracefully catches unmarshaling errors and invalid operation codes without triggering a system panic, protecting the storage layer from corrupted network payloads.

## Network and Routing Layer

- Transparent Write-Proxying: Follower nodes automatically intercept write requests, perform deterministic port offset math to locate the leader, and forward the payload over gRPC.
- Connection Pooling: TCP connection caching was implemented to prevent socket exhaustion during proxy routing, resulting in a measured proxy latency overhead of only 0.9 milliseconds under concurrent load.

## Tunable Read Consistency

Three levels of CAP theorem tradeoffs for client read operations:

- Strong: Forces a TCP quorum check to guarantee linearizability and prevent stale reads.
- Fast: Utilizes Raft leader leases to bypass the network barrier, safely serving local reads.
- Eventual: Allows horizontal read scaling by serving requests directly from follower disks.

### Benchmarks

| Consistency Level   | Latency  | Throughput |
| :------------------ | :------- | :--------- |
| STRONG (quorum)     | 17,309ns | baseline   |
| FAST (lease)        | 10,978ns | +63%       |
| EVENTUAL (follower) | 10,950ns | +63%       |

## Fault Tolerance and Chaos Engineering

- Leader Crash Recovery: Empirically verified via chaos testing that the cluster survives a violent leader process termination. The surviving nodes successfully detect missing heartbeats, execute an election within approximately 100 milliseconds, and fully preserve all previously committed data.
- In-Flight Write Protection: Verified that write requests caught in a severed TCP connection during a crash are subject to strict all-or-nothing transactions. The data is either cleanly dropped, or the write achieved quorum before the crash and was committed. This completely prevents torn writes and silent data corruption.

## Running Locally

Prerequisites: Go 1.21+

To run a 3-node cluster on your local machine, open three separate terminal windows and start each node with its respective ID and port configurations.

Terminal 1 (Node 1 - Initial Leader):

```bash
go run main.go -node-id node1 -raft-port 17001 -grpc-port 18001
```

Terminal 2 (Node 2):

```bash
go run main.go -node-id node2 -raft-port 17002 -grpc-port 18002 -join-addr 127.0.0.1:17001
```

Terminal 3 (Node 3):

```bash
go run main.go -node-id node3 -raft-port 17003 -grpc-port 18003 -join-addr 127.0.0.1:17001
```

## Known Limitations

- Single Raft Group: The current architecture uses a single consensus ring. Horizontal write scaling requires a Multi-Raft architecture (one Raft group per shard).
- Clock Drift Assumptions: The safety of Fast consistency reads relies on bounded clock skew. Severe clock drift between the leader and followers could theoretically result in serving stale data.
- No Cross-Node Transactions: The system guarantees atomicity for single-key operations but does not currently support multi-key ACID transactions across distributed nodes.
- No Authentication: The cluster join endpoint and gRPC proxy layer currently lack mutual TLS or token-based authentication.

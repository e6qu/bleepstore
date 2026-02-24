// Package cluster implements distributed consensus and replication for
// BleepStore using the Raft protocol.
package cluster

import (
	"fmt"
	"log/slog"
)

// RaftNode manages the lifecycle and operations of a single Raft consensus
// node. It coordinates metadata replication across the BleepStore cluster.
type RaftNode struct {
	// NodeID is the unique identifier for this node in the cluster.
	NodeID string
	// BindAddr is the address the Raft transport listens on.
	BindAddr string
	// Peers is the list of peer addresses for cluster bootstrap.
	Peers []string
	// TODO: Add Raft library instance, FSM, log store, stable store, and
	// snapshot store fields.
}

// NewRaftNode creates a new RaftNode with the given configuration.
func NewRaftNode(nodeID, bindAddr string, peers []string) *RaftNode {
	return &RaftNode{
		NodeID:   nodeID,
		BindAddr: bindAddr,
		Peers:    peers,
	}
}

// Start initializes the Raft node, opens the transport, and either bootstraps
// a new cluster or joins an existing one.
//
// TODO: Implement the following steps:
//   - Create a TCP transport on BindAddr.
//   - Initialize the log store, stable store, and snapshot store.
//   - Create the finite state machine (FSM) for metadata replication.
//   - Create the Raft instance.
//   - Bootstrap the cluster if this is the first node, otherwise wait for
//     leader to add this node.
func (n *RaftNode) Start() error {
	slog.Info("RaftNode starting", "node_id", n.NodeID, "bind_addr", n.BindAddr, "peers", n.Peers)
	// TODO: Implement Raft initialization.
	return nil
}

// Stop gracefully shuts down the Raft node, ensuring any pending log entries
// are flushed.
//
// TODO: Implement graceful shutdown:
//   - Signal leadership transfer if this node is the leader.
//   - Shut down the Raft instance.
//   - Close the transport.
func (n *RaftNode) Stop() error {
	slog.Info("RaftNode stopping", "node_id", n.NodeID)
	// TODO: Implement graceful shutdown.
	return nil
}

// Apply proposes a command to the Raft cluster for replicated execution.
// The command is serialized and submitted to the Raft log. If this node is
// not the leader, Apply returns an error indicating the caller should retry
// against the leader.
//
// TODO: Implement command application:
//   - Check if this node is the leader.
//   - Serialize the command.
//   - Submit to Raft via raft.Apply().
//   - Wait for the apply future to complete.
//   - Return any error from the FSM.
func (n *RaftNode) Apply(command []byte) error {
	slog.Debug("RaftNode applying command", "node_id", n.NodeID, "bytes", len(command))
	// TODO: Implement Raft log apply.
	return fmt.Errorf("not implemented")
}

// IsLeader reports whether this node is currently the Raft cluster leader.
func (n *RaftNode) IsLeader() bool {
	// TODO: Check raft.State() == raft.Leader.
	return false
}

// LeaderAddr returns the address of the current Raft cluster leader, or an
// empty string if no leader is known.
func (n *RaftNode) LeaderAddr() string {
	// TODO: Return raft.Leader().
	return ""
}

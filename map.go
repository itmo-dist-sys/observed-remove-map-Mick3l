package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode
	mu           sync.RWMutex
	state        MapState
	counter      uint64
	allNodeIDs   []string
	syncInterval time.Duration
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs_ []string) *CRDTMapNode {
	allNodeIDs := make([]string, 0, len(allNodeIDs_))
	for _, peerID := range allNodeIDs_ {
		if peerID == id {
			continue
		}
		allNodeIDs = append(allNodeIDs, peerID)
	}

	return &CRDTMapNode{
		BaseNode:     hive.NewBaseNode(id),
		state:        make(MapState),
		allNodeIDs:        allNodeIDs,
		syncInterval: 50 * time.Millisecond,
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}

	go n.syncLoop()
	return nil
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	n.counter++
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   Version{Counter: n.counter, NodeID: n.ID()},
	}
	n.mu.Unlock()

	n.broadcastState()
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	n.counter++
	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   Version{Counter: n.counter, NodeID: n.ID()},
	}
	n.mu.Unlock()

	n.broadcastState()
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for key, remoteEntry := range remote {
		localEntry, ok := n.state[key]
		if !ok || compareVersion(remoteEntry.Version, localEntry.Version) > 0 {
			n.state[key] = remoteEntry
		}
		if remoteEntry.Version.Counter > n.counter {
			n.counter = remoteEntry.Version.Counter
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	state := make(MapState, len(n.state))
	for key, entry := range n.state {
		state[key] = entry
	}
	return state
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	view := make(map[string]string)
	for key, entry := range n.state {
		if entry.Tombstone {
			continue
		}
		view[key] = entry.Value
	}
	return view
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	state, ok := msg.Payload.(MapState)
	if !ok {
		return nil
	}
	n.Merge(state)
	return nil
}

func (n *CRDTMapNode) syncLoop() {
	ticker := time.NewTicker(n.syncInterval)
	defer ticker.Stop()

	ctx := n.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.broadcastState()
		}
	}
}

func (n *CRDTMapNode) broadcastState() {
	state := n.State()
	for _, peer := range n.allNodeIDs {
		_ = n.Send(peer, state)
	}
}

func compareVersion(a, b Version) int {
	if a.Counter > b.Counter {
		return 1
	}
	if a.Counter < b.Counter {
		return -1
	}
	if a.NodeID > b.NodeID {
		return 1
	}
	if a.NodeID < b.NodeID {
		return -1
	}
	return 0
}

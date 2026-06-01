package raftnode

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/config"
	"github.com/YonglinLi/config-center/pkg/fsm"
	"github.com/YonglinLi/config-center/pkg/store"
	"github.com/YonglinLi/config-center/pkg/transport"
)

type RaftNode struct {
	Raft      *raft.Raft
	FSM       *fsm.ConfigFSM
	Store     *store.RocksDBStore
	Transport *transport.GRPCTransport
	Config    *config.NodeConfig
}

func NewRaftNode(cfg *config.NodeConfig) (*RaftNode, error) {
	dataDir := cfg.DataDir
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	rocksStore, err := store.NewRocksDBStore(filepath.Join(dataDir, "rocksdb"))
	if err != nil {
		return nil, fmt.Errorf("open rocksdb: %w", err)
	}

	logStore := store.NewRaftLogStore(rocksStore)
	stableStore := store.NewRaftStableStore(rocksStore)

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "snapshots"), 3, os.Stderr)
	if err != nil {
		rocksStore.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	grpcTransport := transport.NewGRPCTransport(
		raft.ServerAddress(cfg.RaftAddr),
		10*time.Second,
	)

	configFSM := fsm.NewConfigFSM(rocksStore)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	if cfg.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.SnapshotInterval > 0 {
		raftConfig.SnapshotInterval = time.Duration(cfg.SnapshotInterval) * time.Second
	}
	if cfg.SnapshotThreshold > 0 {
		raftConfig.SnapshotThreshold = uint64(cfg.SnapshotThreshold)
	}

	r, err := raft.NewRaft(raftConfig, configFSM, logStore, stableStore, snapshotStore, grpcTransport)
	if err != nil {
		rocksStore.Close()
		grpcTransport.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	node := &RaftNode{
		Raft:      r,
		FSM:       configFSM,
		Store:     rocksStore,
		Transport: grpcTransport,
		Config:    cfg,
	}

	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.RaftAddr),
			},
		}
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.Address),
			})
		}

		configuration := raft.Configuration{Servers: servers}
		r.BootstrapCluster(configuration)
	}

	return node, nil
}

func (n *RaftNode) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

func (n *RaftNode) LeaderAddress() string {
	addr, _ := n.Raft.LeaderWithID()
	return string(addr)
}

func (n *RaftNode) LeaderID() string {
	_, id := n.Raft.LeaderWithID()
	return string(id)
}

func (n *RaftNode) Apply(cmd *fsm.Command, timeout time.Duration) (*fsm.CommandResponse, error) {
	data, err := encoding.Encode(cmd)
	if err != nil {
		return nil, err
	}

	future := n.Raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	resp := future.Response().(*fsm.CommandResponse)
	return resp, nil
}

func (n *RaftNode) Shutdown() error {
	f := n.Raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	n.Transport.Close()
	n.Store.Close()
	return nil
}

func (n *RaftNode) GetServers() ([]raft.Server, error) {
	configFuture := n.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, err
	}
	return configFuture.Configuration().Servers, nil
}

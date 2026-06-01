package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	NodeID    string `yaml:"node_id"`
	RaftAddr  string `yaml:"raft_addr"`
	GRPCAddr  string `yaml:"grpc_addr"`
	HTTPAddr  string `yaml:"http_addr"`
	DataDir   string `yaml:"data_dir"`
	Bootstrap bool   `yaml:"bootstrap"`

	Peers []PeerConfig `yaml:"peers"`

	SnapshotInterval  int           `yaml:"snapshot_interval"`
	SnapshotThreshold int           `yaml:"snapshot_threshold"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`

	Auth AuthConfig `yaml:"auth"`
}

type AuthConfig struct {
	AdminTokens []string `yaml:"admin_tokens"`
	AppKeys     []string `yaml:"app_keys"`
}

type PeerConfig struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

func DefaultConfig() *NodeConfig {
	return &NodeConfig{
		NodeID:            "node1",
		RaftAddr:          "localhost:7000",
		GRPCAddr:          "localhost:8000",
		HTTPAddr:          "localhost:9000",
		DataDir:           "./data",
		Bootstrap:         false,
		SnapshotInterval:  120,
		SnapshotThreshold: 8192,
		HeartbeatTimeout:  1000 * time.Millisecond,
		ElectionTimeout:   1000 * time.Millisecond,
		Auth: AuthConfig{
			AdminTokens: []string{"admin-secret-token-change-me"},
			AppKeys:     []string{"app-key-001", "app-key-002"},
		},
	}
}

func LoadConfig(path string) (*NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/fsm"
	"github.com/YonglinLi/config-center/pkg/raftnode"
	"github.com/YonglinLi/config-center/pkg/store"
	pb "github.com/YonglinLi/config-center/proto/config_service"
)

type ConfigServiceServer struct {
	pb.UnimplementedConfigServiceServer
	node *raftnode.RaftNode
}

func NewConfigServiceServer(node *raftnode.RaftNode) *ConfigServiceServer {
	return &ConfigServiceServer{node: node}
}

func (s *ConfigServiceServer) checkLeader(ctx context.Context) error {
	if s.node.IsLeader() {
		return nil
	}
	leaderAddr := s.node.LeaderAddress()
	if leaderAddr == "" {
		return status.Error(codes.Unavailable, "no leader elected")
	}
	header := metadata.Pairs("x-leader-address", leaderAddr)
	grpc.SetHeader(ctx, header)
	return status.Error(codes.FailedPrecondition, "not leader; redirect to: "+leaderAddr)
}

// --- Namespace Management ---

func (s *ConfigServiceServer) CreateNamespace(ctx context.Context, req *pb.CreateNamespaceRequest) (*pb.CreateNamespaceResponse, error) {
	if err := s.checkLeader(ctx); err != nil {
		return nil, err
	}

	cmd := &fsm.Command{
		Type:      fsm.CmdCreateNamespace,
		Namespace: req.Name,
		Comment:   req.Description,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if resp.Error != nil {
		return nil, status.Error(codes.Internal, resp.Error.Error())
	}

	return &pb.CreateNamespaceResponse{Success: true}, nil
}

func (s *ConfigServiceServer) ListNamespaces(ctx context.Context, req *pb.ListNamespacesRequest) (*pb.ListNamespacesResponse, error) {
	it := s.node.Store.NewIteratorCF(store.CFNamespaces)
	defer it.Close()

	var namespaces []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		namespaces = append(namespaces, string(key.Data()))
		key.Free()
	}

	return &pb.ListNamespacesResponse{Namespaces: namespaces}, nil
}

func (s *ConfigServiceServer) DeleteNamespace(ctx context.Context, req *pb.DeleteNamespaceRequest) (*pb.DeleteNamespaceResponse, error) {
	if err := s.checkLeader(ctx); err != nil {
		return nil, err
	}

	cmd := &fsm.Command{
		Type:      fsm.CmdDeleteNamespace,
		Namespace: req.Name,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if resp.Error != nil {
		return nil, status.Error(codes.Internal, resp.Error.Error())
	}

	return &pb.DeleteNamespaceResponse{Success: true}, nil
}

// --- Configuration CRUD ---

func (s *ConfigServiceServer) PutConfig(ctx context.Context, req *pb.PutConfigRequest) (*pb.PutConfigResponse, error) {
	if err := s.checkLeader(ctx); err != nil {
		return nil, err
	}

	cmd := &fsm.Command{
		Type:          fsm.CmdPutConfig,
		Namespace:     req.Namespace,
		Environment:   req.Environment.String(),
		Key:           req.Key,
		Value:         req.Value,
		UpdatedBy:     req.UpdatedBy,
		Comment:       req.Comment,
		ExpectVersion: req.ExpectVersion,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if resp.Error != nil {
		if resp.Error == fsm.ErrVersionConflict {
			return nil, status.Errorf(codes.Aborted, "version conflict: current_version=%d, expect_version=%d", resp.CurrentVersion, req.ExpectVersion)
		}
		return nil, status.Error(codes.Internal, resp.Error.Error())
	}

	return &pb.PutConfigResponse{Version: resp.Version}, nil
}

func (s *ConfigServiceServer) GetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
	if req.Version > 0 {
		return s.getConfigVersion(req.Namespace, req.Environment.String(), req.Key, req.Version)
	}

	baseKey := buildKey(req.Environment.String(), req.Namespace, req.Key)
	data, err := s.node.Store.GetCF(store.CFDefault, []byte(baseKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Error(codes.NotFound, "config not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.GetConfigResponse{Item: entryToProto(&entry)}, nil
}

func (s *ConfigServiceServer) getConfigVersion(namespace, env, key string, version uint64) (*pb.GetConfigResponse, error) {
	versionKey := fmt.Sprintf("%s:%s:%s:%020d", env, namespace, key, version)
	data, err := s.node.Store.GetCF(store.CFVersions, []byte(versionKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Error(codes.NotFound, "version not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.GetConfigResponse{Item: entryToProto(&entry)}, nil
}

func (s *ConfigServiceServer) DeleteConfig(ctx context.Context, req *pb.DeleteConfigRequest) (*pb.DeleteConfigResponse, error) {
	if err := s.checkLeader(ctx); err != nil {
		return nil, err
	}

	cmd := &fsm.Command{
		Type:          fsm.CmdDeleteConfig,
		Namespace:     req.Namespace,
		Environment:   req.Environment.String(),
		Key:           req.Key,
		Comment:       req.Comment,
		ExpectVersion: req.ExpectVersion,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if resp.Error != nil {
		if resp.Error == fsm.ErrVersionConflict {
			return nil, status.Errorf(codes.Aborted, "version conflict: current_version=%d, expect_version=%d", resp.CurrentVersion, req.ExpectVersion)
		}
		return nil, status.Error(codes.Internal, resp.Error.Error())
	}

	return &pb.DeleteConfigResponse{Success: true}, nil
}

func (s *ConfigServiceServer) ListConfigs(ctx context.Context, req *pb.ListConfigsRequest) (*pb.ListConfigsResponse, error) {
	prefix := buildKey(req.Environment.String(), req.Namespace, req.Prefix)

	it := s.node.Store.NewIteratorCF(store.CFDefault)
	defer it.Close()

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	var items []*pb.ConfigItem
	for it.Seek([]byte(prefix)); it.Valid() && len(items) < limit; it.Next() {
		key := it.Key()
		keyStr := string(key.Data())
		key.Free()

		if !strings.HasPrefix(keyStr, prefix) {
			break
		}

		value := it.Value()
		var entry fsm.ConfigEntry
		if err := encoding.Decode(value.Data(), &entry); err != nil {
			value.Free()
			continue
		}
		value.Free()

		items = append(items, entryToProto(&entry))
	}

	return &pb.ListConfigsResponse{Items: items}, nil
}

// --- Version Management ---

func (s *ConfigServiceServer) GetConfigVersion(ctx context.Context, req *pb.GetConfigVersionRequest) (*pb.GetConfigVersionResponse, error) {
	versionKey := fmt.Sprintf("%s:%s:%s:%020d", req.Environment.String(), req.Namespace, req.Key, req.Version)
	data, err := s.node.Store.GetCF(store.CFVersions, []byte(versionKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Error(codes.NotFound, "version not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.GetConfigVersionResponse{Item: entryToProto(&entry)}, nil
}

func (s *ConfigServiceServer) ListConfigVersions(ctx context.Context, req *pb.ListConfigVersionsRequest) (*pb.ListConfigVersionsResponse, error) {
	prefix := fmt.Sprintf("%s:%s:%s:", req.Environment.String(), req.Namespace, req.Key)

	it := s.node.Store.NewIteratorCF(store.CFVersions)
	defer it.Close()

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 50
	}

	var versions []*pb.ConfigItem
	for it.Seek([]byte(prefix)); it.Valid() && len(versions) < limit; it.Next() {
		key := it.Key()
		keyStr := string(key.Data())
		key.Free()

		if !strings.HasPrefix(keyStr, prefix) {
			break
		}

		value := it.Value()
		var entry fsm.ConfigEntry
		if err := encoding.Decode(value.Data(), &entry); err != nil {
			value.Free()
			continue
		}
		value.Free()

		versions = append(versions, entryToProto(&entry))
	}

	return &pb.ListConfigVersionsResponse{Versions: versions}, nil
}

func (s *ConfigServiceServer) RollbackConfig(ctx context.Context, req *pb.RollbackConfigRequest) (*pb.RollbackConfigResponse, error) {
	if err := s.checkLeader(ctx); err != nil {
		return nil, err
	}

	versionKey := fmt.Sprintf("%s:%s:%s:%020d", req.Environment.String(), req.Namespace, req.Key, req.TargetVersion)
	data, err := s.node.Store.GetCF(store.CFVersions, []byte(versionKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Error(codes.NotFound, "target version not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	cmd := &fsm.Command{
		Type:          fsm.CmdPutConfig,
		Namespace:     req.Namespace,
		Environment:   req.Environment.String(),
		Key:           req.Key,
		Value:         entry.Value,
		UpdatedBy:     req.UpdatedBy,
		Comment:       fmt.Sprintf("rollback to version %d", req.TargetVersion),
		ExpectVersion: req.ExpectVersion,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if resp.Error != nil {
		if resp.Error == fsm.ErrVersionConflict {
			return nil, status.Errorf(codes.Aborted, "version conflict: current_version=%d, expect_version=%d; config was modified after your read", resp.CurrentVersion, req.ExpectVersion)
		}
		return nil, status.Error(codes.Internal, resp.Error.Error())
	}

	return &pb.RollbackConfigResponse{NewVersion: resp.Version}, nil
}

// --- Watch ---

func (s *ConfigServiceServer) WatchConfig(req *pb.WatchConfigRequest, stream pb.ConfigService_WatchConfigServer) error {
	ch, cancel := s.node.FSM.Watchers().Subscribe(
		req.Environment.String(),
		req.Namespace,
		req.Key,
	)
	defer cancel()

	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			resp := &pb.WatchConfigResponse{
				Item:      entryToProto(event.Entry),
				EventType: event.EventType,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

// --- Cluster Status ---

func (s *ConfigServiceServer) ClusterStatus(ctx context.Context, req *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	servers, err := s.node.GetServers()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var nodes []*pb.NodeInfo
	for _, srv := range servers {
		nodes = append(nodes, &pb.NodeInfo{
			Id:      string(srv.ID),
			Address: string(srv.Address),
			State:   srv.Suffrage.String(),
		})
	}

	return &pb.ClusterStatusResponse{
		LeaderId:      s.node.LeaderID(),
		LeaderAddress: s.node.LeaderAddress(),
		Nodes:         nodes,
	}, nil
}

// --- Helpers ---

func buildKey(env, namespace, key string) string {
	return env + ":" + namespace + ":" + key
}

func entryToProto(entry *fsm.ConfigEntry) *pb.ConfigItem {
	env := pb.Environment_ENV_UNSPECIFIED
	switch entry.Environment {
	case "ENV_DEV":
		env = pb.Environment_ENV_DEV
	case "ENV_STAGING":
		env = pb.Environment_ENV_STAGING
	case "ENV_PROD":
		env = pb.Environment_ENV_PROD
	}

	item := &pb.ConfigItem{
		Key:         entry.Key,
		Value:       entry.Value,
		Environment: env,
		Namespace:   entry.Namespace,
		Version:     entry.Version,
		UpdatedBy:   entry.UpdatedBy,
		Comment:     entry.Comment,
	}

	if entry.CreatedAt > 0 {
		item.CreatedAt = timestamppb.New(time.Unix(0, entry.CreatedAt))
	}
	if entry.UpdatedAt > 0 {
		item.UpdatedAt = timestamppb.New(time.Unix(0, entry.UpdatedAt))
	}

	return item
}

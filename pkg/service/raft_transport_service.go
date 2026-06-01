package service

import (
	"context"
	"io"
	"time"

	"github.com/hashicorp/raft"

	"github.com/YonglinLi/config-center/pkg/transport"
	pb "github.com/YonglinLi/config-center/proto/raft_transport"
)

type RaftTransportService struct {
	pb.UnimplementedRaftTransportServer
	transport *transport.GRPCTransport
}

func NewRaftTransportService(t *transport.GRPCTransport) *RaftTransportService {
	return &RaftTransportService{transport: t}
}

func (s *RaftTransportService) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	raftReq := decodeAppendEntriesRequest(req)

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  raftReq,
		RespChan: respCh,
	}

	s.transport.HandleRPC(rpc)

	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return nil, resp.Error
		}
		raftResp := resp.Response.(*raft.AppendEntriesResponse)
		return &pb.AppendEntriesResponse{
			Term:           raftResp.Term,
			LastLog:        raftResp.LastLog,
			Success:        raftResp.Success,
			NoRetryBackoff: raftResp.NoRetryBackoff,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RaftTransportService) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	raftReq := &raft.RequestVoteRequest{
		RPCHeader:          raft.RPCHeader{Addr: raft.ServerAddress(req.Candidate)},
		Term:               req.Term,
		LastLogIndex:       req.LastLogIndex,
		LastLogTerm:        req.LastLogTerm,
		LeadershipTransfer: req.LeadershipTransfer,
	}

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  raftReq,
		RespChan: respCh,
	}

	s.transport.HandleRPC(rpc)

	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return nil, resp.Error
		}
		raftResp := resp.Response.(*raft.RequestVoteResponse)
		return &pb.RequestVoteResponse{
			Term:    raftResp.Term,
			Granted: raftResp.Granted,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RaftTransportService) TimeoutNow(ctx context.Context, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	raftReq := &raft.TimeoutNowRequest{
		RPCHeader: raft.RPCHeader{Addr: raft.ServerAddress(req.Leader)},
	}

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  raftReq,
		RespChan: respCh,
	}

	s.transport.HandleRPC(rpc)

	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return &pb.TimeoutNowResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RaftTransportService) InstallSnapshot(stream pb.RaftTransport_InstallSnapshotServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}

	raftReq := &raft.InstallSnapshotRequest{
		RPCHeader:          raft.RPCHeader{Addr: raft.ServerAddress(first.Leader)},
		Term:               first.Term,
		LastLogIndex:       first.LastLogIndex,
		LastLogTerm:        first.LastLogTerm,
		Configuration:      first.Configuration,
		ConfigurationIndex: first.ConfigurationIndex,
		Size:               first.Size_,
	}

	pr, pw := io.Pipe()

	go func() {
		if len(first.Data) > 0 {
			pw.Write(first.Data)
		}
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				pw.Close()
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if len(chunk.Data) > 0 {
				pw.Write(chunk.Data)
			}
		}
	}()

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  raftReq,
		Reader:   pr,
		RespChan: respCh,
	}

	s.transport.HandleRPC(rpc)

	resp := <-respCh
	if resp.Error != nil {
		return stream.SendAndClose(&pb.InstallSnapshotResponse{
			Term:    0,
			Success: false,
		})
	}

	raftResp := resp.Response.(*raft.InstallSnapshotResponse)
	return stream.SendAndClose(&pb.InstallSnapshotResponse{
		Term:    raftResp.Term,
		Success: raftResp.Success,
	})
}

func (s *RaftTransportService) AppendEntriesPipeline(stream pb.RaftTransport_AppendEntriesPipelineServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		raftReq := decodeAppendEntriesRequest(req)

		respCh := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			Command:  raftReq,
			RespChan: respCh,
		}

		s.transport.HandleRPC(rpc)

		resp := <-respCh
		if resp.Error != nil {
			return resp.Error
		}

		raftResp := resp.Response.(*raft.AppendEntriesResponse)
		if err := stream.Send(&pb.AppendEntriesResponse{
			Term:           raftResp.Term,
			LastLog:        raftResp.LastLog,
			Success:        raftResp.Success,
			NoRetryBackoff: raftResp.NoRetryBackoff,
		}); err != nil {
			return err
		}
	}
}

func decodeAppendEntriesRequest(req *pb.AppendEntriesRequest) *raft.AppendEntriesRequest {
	raftReq := &raft.AppendEntriesRequest{
		RPCHeader:         raft.RPCHeader{Addr: raft.ServerAddress(req.Leader)},
		Term:              req.Term,
		PrevLogEntry:      req.PrevLogEntry,
		PrevLogTerm:       req.PrevLogTerm,
		LeaderCommitIndex: req.LeaderCommitIndex,
	}

	for _, entry := range req.Entries {
		raftReq.Entries = append(raftReq.Entries, &raft.Log{
			Index:      entry.Index,
			Term:       entry.Term,
			Type:       raft.LogType(entry.Type),
			Data:       entry.Data,
			Extensions: entry.Extensions,
			AppendedAt: time.Unix(0, entry.AppendedAt),
		})
	}

	return raftReq
}

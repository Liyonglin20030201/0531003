package transport

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	pb "github.com/YonglinLi/config-center/proto/raft_transport"
)

type GRPCTransport struct {
	localAddr        raft.ServerAddress
	consumeCh        chan raft.RPC
	heartbeatCh      chan raft.RPC
	heartbeatHandler func(raft.RPC)
	connPool         *ConnPool
	mu               sync.RWMutex
	timeout          time.Duration
}

func NewGRPCTransport(localAddr raft.ServerAddress, timeout time.Duration) *GRPCTransport {
	return &GRPCTransport{
		localAddr:   localAddr,
		consumeCh:   make(chan raft.RPC),
		heartbeatCh: make(chan raft.RPC),
		connPool:    NewConnPool(),
		timeout:     timeout,
	}
}

func (t *GRPCTransport) Consumer() <-chan raft.RPC {
	return t.consumeCh
}

func (t *GRPCTransport) LocalAddr() raft.ServerAddress {
	return t.localAddr
}

func (t *GRPCTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heartbeatHandler = cb
}

func (t *GRPCTransport) getHeartbeatHandler() func(raft.RPC) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.heartbeatHandler
}

func (t *GRPCTransport) HandleRPC(rpc raft.RPC) {
	if isHeartbeat(rpc) {
		if handler := t.getHeartbeatHandler(); handler != nil {
			handler(rpc)
			return
		}
	}
	t.consumeCh <- rpc
}

func isHeartbeat(rpc raft.RPC) bool {
	req, ok := rpc.Command.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}
	return req.Term != 0 && len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}

func (t *GRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	conn, err := t.connPool.GetConn(string(target))
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftTransportClient(conn)
	ctx := context.Background()
	stream, err := client.AppendEntriesPipeline(ctx)
	if err != nil {
		return nil, err
	}

	return &grpcAppendPipeline{
		stream:    stream,
		inflightCh: make(chan *pipelineInflight, 128),
		doneCh:    make(chan raft.AppendFuture, 128),
	}, nil
}

func (t *GRPCTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	conn, err := t.connPool.GetConn(string(target))
	if err != nil {
		return err
	}

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	pbReq := encodeAppendEntriesRequest(args)
	pbResp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		return err
	}

	decodeAppendEntriesResponse(pbResp, resp)
	return nil
}

func (t *GRPCTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	conn, err := t.connPool.GetConn(string(target))
	if err != nil {
		return err
	}

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	pbReq := encodeRequestVoteRequest(args)
	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	resp.Granted = pbResp.Granted
	return nil
}

func (t *GRPCTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	conn, err := t.connPool.GetConn(string(target))
	if err != nil {
		return err
	}

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	pbReq := &pb.TimeoutNowRequest{
		Leader: []byte(args.RPCHeader.Addr),
	}
	_, err = client.TimeoutNow(ctx, pbReq)
	return err
}

func (t *GRPCTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	conn, err := t.connPool.GetConn(string(target))
	if err != nil {
		return err
	}

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		return err
	}

	first := &pb.InstallSnapshotRequest{
		Leader:             []byte(args.RPCHeader.Addr),
		Term:               args.Term,
		LastLogIndex:       args.LastLogIndex,
		LastLogTerm:        args.LastLogTerm,
		Configuration:      args.Configuration,
		ConfigurationIndex: args.ConfigurationIndex,
		Size_:              args.Size,
	}
	if err := stream.Send(first); err != nil {
		return err
	}

	buf := make([]byte, 64*1024)
	for {
		n, err := data.Read(buf)
		if n > 0 {
			chunk := &pb.InstallSnapshotRequest{
				Data: buf[:n],
			}
			if sendErr := stream.Send(chunk); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	pbResp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	resp.Success = pbResp.Success
	return nil
}

func (t *GRPCTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (t *GRPCTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

func (t *GRPCTransport) Close() error {
	t.connPool.Close()
	return nil
}

// Proto encoding helpers

func encodeAppendEntriesRequest(req *raft.AppendEntriesRequest) *pb.AppendEntriesRequest {
	pbReq := &pb.AppendEntriesRequest{
		Leader:            []byte(req.RPCHeader.Addr),
		Term:              req.Term,
		PrevLogEntry:      req.PrevLogEntry,
		PrevLogTerm:       req.PrevLogTerm,
		LeaderCommitIndex: req.LeaderCommitIndex,
	}

	for _, entry := range req.Entries {
		pbReq.Entries = append(pbReq.Entries, &pb.Log{
			Index:      entry.Index,
			Term:       entry.Term,
			Type:       uint32(entry.Type),
			Data:       entry.Data,
			Extensions: entry.Extensions,
			AppendedAt: entry.AppendedAt.UnixNano(),
		})
	}

	return pbReq
}

func decodeAppendEntriesResponse(pbResp *pb.AppendEntriesResponse, resp *raft.AppendEntriesResponse) {
	resp.Term = pbResp.Term
	resp.LastLog = pbResp.LastLog
	resp.Success = pbResp.Success
	resp.NoRetryBackoff = pbResp.NoRetryBackoff
}

func encodeRequestVoteRequest(req *raft.RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		Candidate:          []byte(req.RPCHeader.Addr),
		Term:               req.Term,
		LastLogIndex:       req.LastLogIndex,
		LastLogTerm:        req.LastLogTerm,
		LeadershipTransfer: req.LeadershipTransfer,
	}
}

// Pipeline implementation

type pipelineInflight struct {
	req  *raft.AppendEntriesRequest
	resp raft.AppendFuture
}

type grpcAppendPipeline struct {
	stream      pb.RaftTransport_AppendEntriesPipelineClient
	inflightCh  chan *pipelineInflight
	doneCh      chan raft.AppendFuture
	closeOnce   sync.Once
}

type appendFuture struct {
	start time.Time
	req   *raft.AppendEntriesRequest
	resp  raft.AppendEntriesResponse
	err   error
	done  chan struct{}
}

func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

func (f *appendFuture) Start() time.Time {
	return f.start
}

func (f *appendFuture) Request() *raft.AppendEntriesRequest {
	return f.req
}

func (f *appendFuture) Response() *raft.AppendEntriesResponse {
	<-f.done
	return &f.resp
}

func (p *grpcAppendPipeline) AppendEntries(req *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	pbReq := encodeAppendEntriesRequest(req)

	if err := p.stream.Send(pbReq); err != nil {
		return nil, err
	}

	future := &appendFuture{
		start: time.Now(),
		req:   req,
		done:  make(chan struct{}),
	}

	go func() {
		pbResp, err := p.stream.Recv()
		if err != nil {
			future.err = err
		} else {
			decodeAppendEntriesResponse(pbResp, &future.resp)
		}
		close(future.done)
	}()

	return future, nil
}

func (p *grpcAppendPipeline) Consumer() <-chan raft.AppendFuture {
	return p.doneCh
}

func (p *grpcAppendPipeline) Close() error {
	p.closeOnce.Do(func() {
		p.stream.CloseSend()
	})
	return nil
}

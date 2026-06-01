package transport

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func NewConnPool() *ConnPool {
	return &ConnPool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (p *ConnPool) GetConn(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	p.conns[addr] = conn
	return conn, nil
}

func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		conn.Close()
		delete(p.conns, addr)
	}
}

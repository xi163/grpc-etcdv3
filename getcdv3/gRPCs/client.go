package gRPCs

import (
	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
)

// <summary>
// ClientConn
// <summary>
type ClientConn interface {
	RPCs() RPCs
	Conn() *grpc.ClientConn
	Free()
	Close()
}

type clientConn struct {
	rpc RPCs
	c   *grpc.ClientConn
}

func NewClientConn(c *grpc.ClientConn) ClientConn {
	return newClientConn(nil, c)
}

func newClientConn(rpc RPCs, c *grpc.ClientConn) ClientConn {
	return &clientConn{rpc: rpc, c: c}
}

func (s *clientConn) RPCs() RPCs {
	return s.rpc
}

func (s *clientConn) Conn() *grpc.ClientConn {
	return s.c
}

func (s *clientConn) Free() {
	switch s.c {
	case nil:
	default:
		switch s.rpc {
		case nil:
			s.close()
		default:
			// logs.Tracef("%v %v %v", s.rpc.Schema(), s.rpc.Node(), s.rpc.Host())
			s.rpc.Put(s)
		}
	}
}

func (s *clientConn) close() {
	switch s.rpc {
	case nil:
		logs.Tracef("")
	default:
		logs.Tracef("%v %v %v", s.rpc.Schema(), s.rpc.Node(), s.rpc.Host())
	}
	_ = s.c.Close()
	s.c = nil
}

func (s *clientConn) Close() {
	switch s.c {
	case nil:
	default:
		s.close()
	}
}

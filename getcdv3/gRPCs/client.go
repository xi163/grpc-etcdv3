package gRPCs

import "google.golang.org/grpc"

// <summary>
// Client
// <summary>
type Client interface {
	Conn() *grpc.ClientConn
	Free()
	Close()
}

// <summary>
// client
// <summary>
type client struct {
	c ClientConn
}

func newClient(c ClientConn) Client {
	return &client{c: c}
}

func (s *client) Conn() *grpc.ClientConn {
	return s.c.Conn()
}

func (s *client) Free() {
	switch s.c {
	case nil:
	default:
		s.c.Free()
		s.c = nil
	}
}

func (s *client) Close() {
	switch s.c {
	case nil:
	default:
		s.c.Close()
		s.c = nil
	}
}

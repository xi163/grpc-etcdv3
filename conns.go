package getcdv3

import (
	"net"
	"strconv"
	"sync"

	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
)

var (
	rpcConns = newRpcConns()
)

// <summary>
// RpcConns
// <summary>
type RpcConns interface {
	Len() (c int)
	Range(cb func(string, *grpc.ClientConn))
	GetConnByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool)
	GetConn(host string) (conn *grpc.ClientConn, ok bool)
	GetConnsByAddr(hosts []Host) (conns []*grpc.ClientConn, valid []Host)
	GetConns(hosts []string) (conns []*grpc.ClientConn, valid []string)
	TryAddConnByAddr(addr string, port int, conn *grpc.ClientConn)
	TryAddConn(host string, conn *grpc.ClientConn)
	RemoveConnByAddr(addr string, port int)
	RemoveConn(host string)
}

// <summary>
// Host
// <summary>
type Host struct {
	Addr string
	Port int
}

// <summary>
// RpcConns_
// <summary>
type RpcConns_ struct {
	l *sync.RWMutex
	m map[string]*grpc.ClientConn
}

func newRpcConns() RpcConns {
	s := &RpcConns_{
		m: map[string]*grpc.ClientConn{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *RpcConns_) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *RpcConns_) Range(cb func(string, *grpc.ClientConn)) {
	s.l.RLock()
	for host, conn := range s.m {
		cb(host, conn)
	}
	s.l.RUnlock()
}

func (s *RpcConns_) GetConnByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool) {
	conn, ok = s.GetConn(net.JoinHostPort(addr, strconv.Itoa(port)))
	return
}

func (s *RpcConns_) GetConn(host string) (conn *grpc.ClientConn, ok bool) {
	s.l.RLock()
	conn, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *RpcConns_) GetConnsByAddr(hosts []Host) (conns []*grpc.ClientConn, valid []Host) {
	s.l.RLock()
	for _, host := range hosts {
		conn, ok := s.m[net.JoinHostPort(host.Addr, strconv.Itoa(host.Port))]
		switch ok {
		case true:
			conns = append(conns, conn)
		default:
			valid = append(valid, host)
		}
	}
	s.l.RUnlock()
	return
}

func (s *RpcConns_) GetConns(hosts []string) (conns []*grpc.ClientConn, valid []string) {
	s.l.RLock()
	for _, host := range hosts {
		conn, ok := s.m[host]
		switch ok {
		case true:
			conns = append(conns, conn)
		default:
			valid = append(valid, host)
		}
	}
	s.l.RUnlock()
	return
}

func (s *RpcConns_) TryAddConnByAddr(addr string, port int, conn *grpc.ClientConn) {
	s.TryAddConn(net.JoinHostPort(addr, strconv.Itoa(port)), conn)
}

func (s *RpcConns_) TryAddConn(host string, conn *grpc.ClientConn) {
	switch s.existConn(host) {
	case true:
	default:
		s.tryAddConn(host, conn)
	}
}

func (s *RpcConns_) existConn(host string) (ok bool) {
	s.l.RLock()
	_, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *RpcConns_) tryAddConn(host string, conn *grpc.ClientConn) {
	s.l.Lock()
	_, ok := s.m[host]
	switch ok {
	case true:
	default:
		s.m[host] = conn
	}
	s.l.Unlock()
}

func (s *RpcConns_) RemoveConnByAddr(addr string, port int) {
	s.RemoveConn(net.JoinHostPort(addr, strconv.Itoa(port)))
}

func (s *RpcConns_) RemoveConn(host string) {
	switch s.exist(host) {
	case true:
		s.removeConn(host)
	default:
	}
}

func (s *RpcConns_) exist(host string) (ok bool) {
	s.l.RLock()
	_, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *RpcConns_) removeConn(host string) {
	logs.Debugf("%v begin size=%v", host, s.Len())
	s.l.Lock()
	delete(s.m, host)
	s.l.Unlock()
	logs.Warnf("%v end size=%v", host, s.Len())
}

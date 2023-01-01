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
	GetByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool)
	Get(host string) (conn *grpc.ClientConn, ok bool)
	GetConnsByAddr(hosts []Host) (conns []*grpc.ClientConn, valid []Host)
	GetConns(hosts []string) (conns []*grpc.ClientConn, valid []string)
	TryAddByAddr(addr string, port int, conn *grpc.ClientConn)
	TryAdd(host string, conn *grpc.ClientConn)
	RemoveByAddr(addr string, port int)
	Remove(host string)
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

func (s *RpcConns_) GetByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool) {
	conn, ok = s.Get(net.JoinHostPort(addr, strconv.Itoa(port)))
	return
}

func (s *RpcConns_) Get(host string) (conn *grpc.ClientConn, ok bool) {
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

func (s *RpcConns_) TryAddByAddr(addr string, port int, conn *grpc.ClientConn) {
	s.TryAdd(net.JoinHostPort(addr, strconv.Itoa(port)), conn)
}

func (s *RpcConns_) TryAdd(host string, conn *grpc.ClientConn) {
	_, ok := s.Get(host)
	switch ok {
	case true:
	default:
		s.tryAdd(host, conn)
	}
}

func (s *RpcConns_) tryAdd(host string, conn *grpc.ClientConn) {
	s.l.Lock()
	_, ok := s.m[host]
	switch ok {
	case true:
	default:
		s.m[host] = conn
	}
	s.l.Unlock()
}

func (s *RpcConns_) RemoveByAddr(addr string, port int) {
	s.Remove(net.JoinHostPort(addr, strconv.Itoa(port)))
}

func (s *RpcConns_) Remove(host string) {
	_, ok := s.Get(host)
	switch ok {
	case true:
		s.remove(host)
	default:
	}
}

func (s *RpcConns_) remove(host string) {
	logs.Debugf("%v begin size=%v", host, s.Len())
	s.l.Lock()
	delete(s.m, host)
	s.l.Unlock()
	logs.Warnf("%v end size=%v", host, s.Len())
}

package getcdv3

import (
	"net"
	"strconv"
	"sync"

	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
)

var (
	rpcConns = newClientConns()
)

// <summary>
// Host
// <summary>
type Host struct {
	Addr string
	Port int
}

// <summary>
// ClientConns
// <summary>
type ClientConns struct {
	l *sync.RWMutex
	m map[string]*grpc.ClientConn
}

func newClientConns() *ClientConns {
	s := &ClientConns{
		m: map[string]*grpc.ClientConn{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *ClientConns) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *ClientConns) Range(cb func(string, *grpc.ClientConn)) {
	s.l.RLock()
	for host, conn := range s.m {
		cb(host, conn)
	}
	s.l.RUnlock()
}

func (s *ClientConns) GetConnByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool) {
	s.l.RLock()
	conn, ok = s.m[net.JoinHostPort(addr, strconv.Itoa(port))]
	s.l.RUnlock()
	return
}

func (s *ClientConns) GetConnByHost(host string) (conn *grpc.ClientConn, ok bool) {
	s.l.RLock()
	conn, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *ClientConns) GetConnsByAddr(hosts []Host) (conns []*grpc.ClientConn, valid []Host) {
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

func (s *ClientConns) GetConnsByHost(hosts []string) (conns []*grpc.ClientConn, valid []string) {
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

func (s *ClientConns) existConnByAddr(addr string, port int) (ok bool) {
	s.l.RLock()
	_, ok = s.m[net.JoinHostPort(addr, strconv.Itoa(port))]
	s.l.RUnlock()
	return
}

func (s *ClientConns) TryAddConnByAddr(addr string, port int, conn *grpc.ClientConn) {
	switch s.existConnByAddr(addr, port) {
	case true:
	default:
		s.tryAddConnByAddr(addr, port, conn)
	}
}

func (s *ClientConns) tryAddConnByAddr(addr string, port int, conn *grpc.ClientConn) {
	s.l.Lock()
	_, ok := s.m[net.JoinHostPort(addr, strconv.Itoa(port))]
	switch ok {
	case true:
	default:
		s.m[net.JoinHostPort(addr, strconv.Itoa(port))] = conn
	}
	s.l.Unlock()
}

func (s *ClientConns) existConnByHost(host string) (ok bool) {
	s.l.RLock()
	_, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *ClientConns) TryAddConnByHost(host string, conn *grpc.ClientConn) {
	switch s.existConnByHost(host) {
	case true:
	default:
		s.tryAddConnByHost(host, conn)
	}
}

func (s *ClientConns) tryAddConnByHost(host string, conn *grpc.ClientConn) {
	s.l.Lock()
	_, ok := s.m[host]
	switch ok {
	case true:
	default:
		s.m[host] = conn
	}
	s.l.Unlock()
}

func (s *ClientConns) RemoveConnByAddr(addr string, port int) {
	logs.Debugf("%v begin size=%v", net.JoinHostPort(addr, strconv.Itoa(port)), s.Len())
	// s.Range(func(host string, _ *grpc.ClientConn) {
	// 	logs.Warnf("%v", host)
	// })
	s.l.Lock()
	delete(s.m, net.JoinHostPort(addr, strconv.Itoa(port)))
	s.l.Unlock()
	logs.Warnf("%v end size=%v", net.JoinHostPort(addr, strconv.Itoa(port)), s.Len())
	// s.Range(func(host string, _ *grpc.ClientConn) {
	// 	logs.Warnf("%v", host)
	// })
}

func (s *ClientConns) RemoveConnByHost(host string) {
	logs.Debugf("%v begin size=%v", host, s.Len())
	// s.Range(func(host string, _ *grpc.ClientConn) {
	// 	logs.Debugf("%v", host)
	// })
	s.l.Lock()
	delete(s.m, host)
	s.l.Unlock()
	logs.Warnf("%v end size=%v", host, s.Len())
	// s.Range(func(host string, _ *grpc.ClientConn) {
	// 	logs.Warnf("%v", host)
	// })
}

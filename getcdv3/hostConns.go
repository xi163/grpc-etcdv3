package getcdv3

import (
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
)

// <summary>
// Conns
// <summary>
type Conns interface {
	Len() (c int)
	Range(cb func(string, *grpc.ClientConn) bool)
	GetByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool)
	Get(host string) (conn *grpc.ClientConn, ok bool)
	GetConns(hosts map[string]bool) (conns []*grpc.ClientConn, slice map[string]bool)
	TryAddByAddr(addr string, port int, conn *grpc.ClientConn)
	TryAdd(host string, conn *grpc.ClientConn)
	RemoveByAddr(addr string, port int)
	Remove(host string) (ok bool)
	Update(hosts map[string]bool) bool
}

// <summary>
// Conns_
// <summary>
type Conns_ struct {
	target string
	cb     func(string)
	l      *sync.RWMutex
	m      map[string]*grpc.ClientConn
}

func newConns(target string, cb func(string)) Conns {
	s := &Conns_{
		target: target,
		cb:     cb,
		m:      map[string]*grpc.ClientConn{},
		l:      &sync.RWMutex{},
	}
	return s
}

func (s *Conns_) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *Conns_) Range(cb func(string, *grpc.ClientConn) bool) {
	s.l.RLock()
	for host, conn := range s.m {
		switch cb(host, conn) {
		case true:
		default:
			s.l.RUnlock()
			return
		}
	}
	s.l.RUnlock()
}

func (s *Conns_) GetByAddr(addr string, port int) (conn *grpc.ClientConn, ok bool) {
	conn, ok = s.Get(net.JoinHostPort(addr, strconv.Itoa(port)))
	return
}

func (s *Conns_) Get(host string) (conn *grpc.ClientConn, ok bool) {
	s.l.RLock()
	conn, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *Conns_) GetConns(hosts map[string]bool) (conns []*grpc.ClientConn, slice map[string]bool) {
	slice = map[string]bool{}
	s.l.RLock()
	for host := range hosts {
		conn, ok := s.m[host]
		switch ok {
		case true:
			conns = append(conns, conn)
		default:
			slice[host] = true
		}
	}
	s.l.RUnlock()
	return
}

func (s *Conns_) TryAddByAddr(addr string, port int, conn *grpc.ClientConn) {
	s.TryAdd(net.JoinHostPort(addr, strconv.Itoa(port)), conn)
}

func (s *Conns_) TryAdd(host string, conn *grpc.ClientConn) {
	_, ok := s.Get(host)
	switch ok {
	case true:
	default:
		s.tryAdd(host, conn)
	}
}

func (s *Conns_) tryAdd(host string, conn *grpc.ClientConn) {
	s.l.Lock()
	_, ok := s.m[host]
	switch ok {
	case true:
	default:
		s.m[host] = conn
	}
	s.l.Unlock()
}

func (s *Conns_) remove(host string) (c int, ok bool) {
	s.l.Lock()
	_, ok = s.m[host]
	switch ok {
	case true:
		delete(s.m, host)
	}
	c = len(s.m)
	s.l.Unlock()
	return
}

func (s *Conns_) RemoveByAddr(addr string, port int) {
	s.Remove(net.JoinHostPort(addr, strconv.Itoa(port)))
}

func (s *Conns_) Remove(host string) (ok bool) {
	_, ok = s.Get(host)
	switch ok {
	case true:
		c, OK := s.remove(host)
		ok = OK
		switch c {
		case 0:
			s.cb(s.target)
		}
	default:
	}
	return
}

func (s *Conns_) RangeRemoveWithCond(cb func(string, *grpc.ClientConn) bool) (update bool) {
	s.l.Lock()
	for host, conn := range s.m {
		switch cb(host, conn) {
		case true:
			delete(s.m, host)
			update = true
		}
	}
	c := len(s.m)
	s.l.Unlock()
	switch update {
	case true:
		switch c {
		case 0:
			s.cb(s.target)
		}
	}
	return
}

func (s *Conns_) Update(hosts map[string]bool) bool {
	return s.RangeRemoveWithCond(func(host string, _ *grpc.ClientConn) bool {
		_, ok := hosts[host]
		return !ok
	})
}

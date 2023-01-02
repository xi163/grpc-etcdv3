package getcdv3

import (
	"net"
	"strings"
	"sync"

	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
)

var (
	rpcConns = newRpcConns()
)

func RemoveBy(err error) {
	cache := true
	switch cache {
	case true:
	default:
		rpcConns.RemoveBy(err)
	}
}

// <summary>
// RpcConns
// <summary>
type RpcConns interface {
	Len() (c int)
	Range(cb func(string, Conns) bool)
	Get(target string) (conns Conns, ok bool)
	GetAdd(target string) (conns Conns, ok bool)
	Update(target string, hosts map[string]bool)
	Remove(target string)
	RemoveBy(err error)
	List()
}

// <summary>
// RpcConns_
// <summary>
type RpcConns_ struct {
	l *sync.RWMutex
	m map[string]Conns
}

func newRpcConns() RpcConns {
	s := &RpcConns_{
		m: map[string]Conns{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *RpcConns_) List() {
	s.Range(func(target string, conns Conns) bool {
		logs.Errorf("------------------------------- %v", target)
		conns.Range(func(host string, conn *grpc.ClientConn) bool {
			logs.Errorf("%v", host)
			return true
		})
		logs.Errorf("-------------------------------")
		return true
	})
}

func (s *RpcConns_) Update(target string, hosts map[string]bool) {
	conns, ok := s.Get(target)
	switch ok {
	case true:
		switch conns.Update(hosts) {
		case true:
			s.List()
		}
	default:
	}
}

func (s *RpcConns_) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *RpcConns_) Range(cb func(string, Conns) bool) {
	s.l.RLock()
	for target, conns := range s.m {
		switch cb(target, conns) {
		case true:
		default:
			s.l.RUnlock()
			return
		}
	}
	s.l.RUnlock()
}

func (s *RpcConns_) Get(target string) (conns Conns, ok bool) {
	s.l.RLock()
	conns, ok = s.m[target]
	s.l.RUnlock()
	return
}

func (s *RpcConns_) GetAdd(target string) (conns Conns, ok bool) {
	conns, ok = s.Get(target)
	switch ok {
	case true:
	default:
		conns, ok = s.getAdd(target)
	}
	return
}

func (s *RpcConns_) getAdd(target string) (conns Conns, ok bool) {
	s.l.Lock()
	conns, ok = s.m[target]
	switch ok {
	case true:
	default:
		conns = newConns(target, s.Remove)
		s.m[target] = conns
		ok = true
	}
	s.l.Unlock()
	return
}

func (s *RpcConns_) Remove(target string) {
	_, ok := s.Get(target)
	switch ok {
	case true:
		s.remove(target)
	default:
	}
}

func (s *RpcConns_) remove(target string) {
	s.l.Lock()
	_, ok := s.m[target]
	switch ok {
	case true:
		delete(s.m, target)
	}
	s.l.Unlock()
}

func (s *RpcConns_) RemoveBy(err error) {
	switch err {
	case nil:
	default:
		sub := ""
		match := "dial tcp"
		host := ""
		idx := strings.Index(err.Error(), match)
		switch idx >= 0 {
		case true:
			sub = err.Error()[idx+len(match):]
			sub = strings.Replace(sub, " ", "", 1)
			slice := strings.Split(sub, ":")
			switch len(slice) >= 2 {
			case true:
				host = net.JoinHostPort(slice[0], slice[1])
			}
		}
		switch host {
		case "":
		default:
			s.Range(func(target string, conns Conns) bool {
				return !conns.Remove(host)
			})
		}
	}
}

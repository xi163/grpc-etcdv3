package gRPCs

import (
	"net"
	"strings"
	"sync"

	"github.com/cwloo/gonet/logs"
)

var (
	rpcConns = newConns()
)

func Conns() RpcConns {
	return rpcConns
}

func RemoveBy(err error) {
	cache := true
	switch cache {
	case true:
	default:
		rpcConns.RemoveBy(err)
	}
}

// <summary>
// RpcConns [target]=Clients
// <summary>
type RpcConns interface {
	Len() (c int)
	Range(cb func(string, Clients) bool)
	Get(target string) (c Clients, ok bool)
	GetAdd(target string) (c Clients, ok bool)
	Update(target string, hosts map[string]bool)
	Remove(target string)
	RemoveBy(err error)
	Close()
	List()
}

// <summary>
// conns
// <summary>
type conns struct {
	l *sync.RWMutex
	m map[string]Clients
}

func newConns() RpcConns {
	s := &conns{
		m: map[string]Clients{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *conns) List() {
	s.Range(func(target string, clients Clients) bool {
		logs.Errorf("------------------------------- %v", target)
		clients.Range(func(host string, c RPCs) bool {
			logs.Errorf("%v", host)
			return true
		})
		logs.Errorf("-------------------------------")
		return true
	})
}

func (s *conns) Update(target string, hosts map[string]bool) {
	conns, ok := s.Get(target)
	switch ok {
	case true:
		switch conns.Update(hosts) {
		case true:
			// s.List()
		}
	default:
	}
}

func (s *conns) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *conns) Range(cb func(string, Clients) bool) {
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

func (s *conns) Get(target string) (c Clients, ok bool) {
	s.l.RLock()
	c, ok = s.m[target]
	s.l.RUnlock()
	return
}

func (s *conns) GetAdd(target string) (c Clients, ok bool) {
	c, ok = s.Get(target)
	switch ok {
	case true:
	default:
		c, ok = s.getAdd(target)
	}
	return
}

func (s *conns) getAdd(target string) (c Clients, ok bool) {
	s.l.Lock()
	c, ok = s.m[target]
	switch ok {
	case true:
	default:
		c = newClients(target, s.Remove)
		s.m[target] = c
		ok = true
	}
	s.l.Unlock()
	return
}

func (s *conns) Remove(target string) {
	_, ok := s.Get(target)
	switch ok {
	case true:
		switch s.remove(target) {
		case true:
			// s.List()
		}
	default:
	}
}

func (s *conns) remove(target string) (ok bool) {
	s.l.Lock()
	_, ok = s.m[target]
	switch ok {
	case true:
		delete(s.m, target)
	}
	s.l.Unlock()
	return
}

func (s *conns) RemoveBy(err error) {
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
			s.Range(func(target string, conns Clients) bool {
				return !conns.Remove(host)
			})
		}
	}
}

func (s *conns) Close() {
	s.Range(func(target string, clients Clients) bool {
		logs.Errorf("------------------------------- %v", target)
		clients.Range(func(host string, c RPCs) bool {
			logs.Errorf("%v", host)
			c.Close(func(c ClientConn) {})
			return true
		})
		logs.Errorf("-------------------------------")
		return true
	})
}

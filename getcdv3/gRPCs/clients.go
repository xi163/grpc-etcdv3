package gRPCs

import (
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
)

func Convert(c *grpc.ClientConn) Client {
	return newClient(newClientConn(nil, c))
}

// <summary>
// Clients [host]=RPCs
// <summary>
type Clients interface {
	List()
	Len() (c int)
	Range(cb func(string, RPCs) bool)
	GetByAddr(addr string, port int) (p RPCs, ok bool)
	Get(host string) (p RPCs, ok bool)
	GetAddByAddr(unique bool, schema, node, addr string, port int, dial Dial) (p RPCs, ok bool)
	GetAdd(unique bool, schema, node, host string, dial Dial) (p RPCs, ok bool)
	RemoveByAddr(addr string, port int)
	Remove(host string) (ok bool)
	Update(hosts map[string]bool) bool
	GetConnByAddr(addr string, port int) (c Client, err error)
	GetConn(host string) (c Client, err error)
	GetConns(hosts map[string]bool) (conns []Client, slice map[string]bool)
	Convert(unique bool, schema, node, host string, dial Dial, c *grpc.ClientConn) (conn Client)
}

type clients struct {
	target string
	cb     func(string)
	m      map[string]RPCs
	l      *sync.RWMutex
}

func newClients(target string, cb func(string)) Clients {
	return &clients{
		target: target,
		cb:     cb,
		m:      map[string]RPCs{},
		l:      &sync.RWMutex{},
	}
}

func (s *clients) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *clients) Range(cb func(string, RPCs) bool) {
	s.l.RLock()
	for host, c := range s.m {
		switch cb(host, c) {
		case true:
		default:
			s.l.RUnlock()
			return
		}
	}
	s.l.RUnlock()
}

func (s *clients) List() {
	logs.Errorf("------------------------------- %v", s.target)
	s.Range(func(host string, c RPCs) bool {
		logs.Errorf("%v", host)
		return true
	})
	logs.Errorf("-------------------------------")
}

func (s *clients) GetByAddr(addr string, port int) (c RPCs, ok bool) {
	c, ok = s.Get(net.JoinHostPort(addr, strconv.Itoa(port)))
	return
}

func (s *clients) Get(host string) (c RPCs, ok bool) {
	s.l.RLock()
	c, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *clients) GetAddByAddr(unique bool, schema, node, addr string, port int, dial Dial) (c RPCs, ok bool) {
	c, ok = s.GetAdd(unique, schema, node, net.JoinHostPort(addr, strconv.Itoa(port)), dial)
	return
}

func (s *clients) GetAdd(unique bool, schema, node, host string, dial Dial) (c RPCs, ok bool) {
	c, ok = s.Get(host)
	switch ok {
	case true:
	default:
		c, ok = s.getAdd(unique, schema, node, host, dial)
	}
	return
}

func (s *clients) getAdd(unique bool, schema, node, host string, dial Dial) (c RPCs, ok bool) {
	s.l.Lock()
	c, ok = s.m[host]
	switch ok {
	case true:
	default:
		c = newRPCs(unique, schema, node, host, dial)
		s.m[host] = c
		ok = true
	}
	s.l.Unlock()
	return
}

func (s *clients) remove(host string, reset func(RPCs)) (c int, rpc RPCs, ok bool) {
	s.l.Lock()
	rpc, ok = s.m[host]
	switch ok {
	case true:
		delete(s.m, host)
		c = len(s.m)
		s.l.Unlock()
		goto OK
	}
	c = len(s.m)
	s.l.Unlock()
	return
OK:
	reset(rpc)
	return
}

func (s *clients) RemoveByAddr(addr string, port int) {
	s.Remove(net.JoinHostPort(addr, strconv.Itoa(port)))
}

func (s *clients) Remove(host string) (ok bool) {
	_, ok = s.remove_(host)
	return
}

func (s *clients) remove_(host string) (c int, ok bool) {
	_, ok = s.Get(host)
	switch ok {
	case true:
		c, _, ok = s.remove(host, func(rpc RPCs) {
			rpc.Close(func(_ ClientConn) {})
		})
		switch ok {
		case true:
			// s.List()
			switch c {
			case 0:
				s.cb(s.target)
			}
		}
	default:
	}
	return
}

func (s *clients) RangeRemoveWithCond(cb func(string, RPCs) bool, reset func([]RPCs)) (update bool) {
	rpcs := []RPCs{}
	s.l.Lock()
	for host, rpc := range s.m {
		switch cb(host, rpc) {
		case true:
			rpcs = append(rpcs, rpc)
			delete(s.m, host)
			update = true
		}
	}
	c := len(s.m)
	s.l.Unlock()
	switch update {
	case true:
		reset(rpcs)
		// s.List()
		switch c {
		case 0:
			s.cb(s.target)
		}
	}
	return
}

func (s *clients) Update(hosts map[string]bool) bool {
	return s.RangeRemoveWithCond(func(host string, _ RPCs) bool {
		_, ok := hosts[host]
		return !ok
	}, func(rpcs []RPCs) {
		for _, rpc := range rpcs {
			rpc.Close(func(_ ClientConn) {})
		}
	})
}

func (s *clients) GetConnByAddr(addr string, port int) (c Client, err error) {
	c, err = s.GetConn(net.JoinHostPort(addr, strconv.Itoa(port)))
	return
}

func (s *clients) GetConn(host string) (Client, error) {
	c, err := s.getConn(host)
	switch err {
	case nil:
		return newClient(c), err
	default:
		return nil, err
	}
}

func (s *clients) getConn(host string) (c ClientConn, err error) {
	rpc, ok := s.Get(host)
	switch ok {
	case true:
		c, err = rpc.Get()
		switch err {
		case nil:
		default:
			s.Remove(host)
		}
	default:
		err = errors.New(logs.SprintErrorf("error"))
	}
	return
}

func (s *clients) GetConns(hosts map[string]bool) (conns []Client, slice map[string]bool) {
	remove := map[string]bool{}
	slice = map[string]bool{}
	s.l.RLock()
	for host := range hosts {
		rpc, ok := s.m[host]
		switch ok {
		case true:
			c, err := rpc.Get()
			switch err {
			case nil:
				conns = append(conns, newClient(c))
			default:
				remove[host] = true
			}
		default:
			slice[host] = true
		}
	}
	s.l.RUnlock()
	for host := range remove {
		s.Remove(host)
	}
	return
}

func (s *clients) Convert(unique bool, schema, node, host string, dial Dial, c *grpc.ClientConn) (conn Client) {
	rpc, ok := s.GetAdd(unique, schema, node, host, dial)
	switch ok {
	case true:
		switch rpc.Len() {
		case 0:
			conn = newClient(newClientConn(rpc, c))
		default:
			conn = newClient(newClientConn(nil, c))
		}
	default:
		logs.Fatalf("error")
	}
	return
}

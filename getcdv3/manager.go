package getcdv3

import (
	"net"
	"strings"
	"sync"

	"github.com/cwloo/gonet/logs"
	grpc_resolver "google.golang.org/grpc/resolver"
)

var (
	builders = newManager()
)

// <summary>
// Manager [schema]=Builder
// <summary>
type Manager interface {
	Len() (c int)
	Range(cb func(string, Builder))
	Get(schema string) (b Builder, ok bool)
	GetAdd(schema string) (b Builder, ok bool)
	Remove(schema string, cb func(string, Builder))
	RangeRemove(cb func(string, Builder))
	Close()
}

// <summary>
// manager
// <summary>
type manager struct {
	m map[string]Builder
	l *sync.RWMutex
}

func newManager() Manager {
	s := &manager{
		m: map[string]Builder{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *manager) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *manager) Range(cb func(string, Builder)) {
	s.l.RLock()
	for schema, b := range s.m {
		cb(schema, b)
	}
	s.l.RUnlock()
}

func (s *manager) Get(schema string) (b Builder, ok bool) {
	s.l.RLock()
	b, ok = s.m[schema]
	s.l.RUnlock()
	return
}

func (s *manager) GetAdd(schema string) (b Builder, ok bool) {
	b, ok = s.Get(schema)
	switch ok {
	case true:
	default:
		b, ok = s.getAdd(schema)
	}
	return
}

func (s *manager) getAdd(schema string) (b Builder, ok bool) {
	s.l.Lock()
	b, ok = s.m[schema]
	switch ok {
	case true:
	default:
		b = newBuilder(schema)
		grpc_resolver.Register(b)
		s.m[schema] = b
		ok = true
	}
	s.l.Unlock()
	return
}

func (s *manager) remove(schema string, cb func(string, Builder)) {
	s.l.Lock()
	b, ok := s.m[schema]
	switch ok {
	case true:
		switch schema == b.Scheme() {
		case false:
			s.l.Unlock()
			goto ERR
		}
		cb(schema, b)
		grpc_resolver.UnregisterForTesting(schema)
		delete(s.m, schema)
	}
	s.l.Unlock()
	return
ERR:
	logs.Fatalf("error %v %v", schema, b.Scheme())
}

func (s *manager) Remove(schema string, cb func(string, Builder)) {
	_, ok := s.Get(schema)
	switch ok {
	case true:
		s.remove(schema, cb)
	default:
	}
}

func (s *manager) RangeRemove(cb func(string, Builder)) {
	s.l.Lock()
	for schema, b := range s.m {
		cb(schema, b)
		grpc_resolver.UnregisterForTesting(schema)
		delete(s.m, schema)
	}
	s.l.Unlock()
}

func (s *manager) Close() {
	s.RangeRemove(func(_ string, b Builder) {
		b.Close()
	})
}

func TargetString(unique bool, schema, serviceName string) string {
	switch unique {
	case true:
		return GetPrefix4Unique(schema, serviceName)
	default:
		return GetPrefix(schema, serviceName)
	}
}

func Slice(d map[string]bool) (v []string) {
	for k := range d {
		v = append(v, k)
	}
	return
}

func TargetToHost(target string) (unique bool, schema, serviceName, host string) {
	sub := ""
	match := ":///"
	idx := strings.Index(target, match)
	switch idx >= 0 {
	case true:
		schema = target[:idx]
		sub = target[idx+len(match):]
		slice := strings.Split(sub, ":")
		switch len(slice) {
		case 1:
			idx := strings.LastIndex(slice[0], "/")
			unique = idx < 0
			switch unique {
			case true:
				serviceName = slice[0]
			default:
				serviceName = slice[0][:idx]
			}
		case 3:
			serviceName = slice[0]
			idx := strings.LastIndex(slice[2], "/")
			unique = idx < 0
			switch unique {
			case true:
				host = net.JoinHostPort(slice[1], slice[2])
			default:
				host = net.JoinHostPort(slice[1], slice[2][0:idx])
			}
		}
	}
	return
}

package getcdv3

import (
	"sync"

	"github.com/cwloo/gonet/logs"
	grpc_resolver "google.golang.org/grpc/resolver"
)

var (
	manager = newManager()
)

// <summary>
// Manager
// <summary>
type Manager struct {
	m map[string]*Builder
	l *sync.RWMutex
}

func newManager() *Manager {
	s := &Manager{
		m: map[string]*Builder{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *Manager) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *Manager) Range(cb func(string, *Builder)) {
	s.l.RLock()
	for schema, b := range s.m {
		cb(schema, b)
	}
	s.l.RUnlock()
}

func (s *Manager) Get(schema string) (b *Builder, ok bool) {
	// logs.Debugf("%v begin size=%v", schema, s.Len())
	s.l.RLock()
	b, ok = s.m[schema]
	s.l.RUnlock()
	// logs.Debugf("%v end size=%v", schema, s.Len())
	return
}

func (s *Manager) GetAdd(schema string) (b *Builder, ok bool) {
	b, ok = s.Get(schema)
	switch ok {
	case true:
	default:
		b, ok = s.getAdd(schema)
	}
	return
}

func (s *Manager) getAdd(schema string) (b *Builder, ok bool) {
	// logs.Debugf("%v begin size=%v", schema, s.Len())
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
	// logs.Debugf("%v end size=%v", schema, s.Len())
	return
}

func (s *Manager) RemoveWith(schema string, cb func(string, *Builder)) {
	s.l.Lock()
	b, ok := s.m[schema]
	switch ok {
	case true:
		switch schema == b.Scheme() {
		case false:
			s.l.Unlock()
			goto ERR
		}
		logs.Errorf("%v begin size=%v", schema, len(s.m))
		cb(schema, b)
		grpc_resolver.UnregisterForTesting(schema)
		delete(s.m, schema)
		logs.Errorf("%v end size=%v", schema, len(s.m))
	}
	s.l.Unlock()
ERR:
	logs.Fatalf("error")
}

func (s *Manager) Remove(schema string) {
	s.l.Lock()
	b, ok := s.m[schema]
	switch ok {
	case true:
		switch schema == b.Scheme() {
		case false:
			s.l.Unlock()
			goto ERR
		}
		logs.Errorf("%v begin size=%v", schema, len(s.m))
		grpc_resolver.UnregisterForTesting(schema)
		delete(s.m, schema)
		logs.Errorf("%v end size=%v", schema, len(s.m))
	}
	s.l.Unlock()
ERR:
	logs.Fatalf("error")
}

func (s *Manager) RangeRemoveWith(cb func(string, *Builder)) {
	s.l.Lock()
	for schema, b := range s.m {
		logs.Errorf("%v begin size=%v", schema, len(s.m))
		cb(schema, b)
		grpc_resolver.UnregisterForTesting(schema)
		delete(s.m, schema)
		logs.Errorf("%v end size=%v", schema, len(s.m))
	}
	s.l.Unlock()
}

func (s *Manager) RangeRemove() {
	s.l.Lock()
	for schema := range s.m {
		logs.Errorf("%v begin size=%v", schema, len(s.m))
		grpc_resolver.UnregisterForTesting(schema)
		delete(s.m, schema)
		logs.Errorf("%v end size=%v", schema, len(s.m))
	}
	s.l.Unlock()
}

func (s *Manager) Close() {
	logs.Debugf("")
	s.RangeRemoveWith(func(_ string, b *Builder) {
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

package getcdv3

import (
	"sync"

	"github.com/cwloo/gonet/logs"
	grpc_resolver "google.golang.org/grpc/resolver"
)

var (
	resolver = newResolver()
)

// <summary>
// Resolver
// <summary>
type Resolver struct {
	grpc_resolver.Resolver
	l *sync.RWMutex
	m map[string]*Builder
}

func newResolver() *Resolver {
	s := &Resolver{
		m: map[string]*Builder{},
		l: &sync.RWMutex{},
	}
	return s
}

func (s *Resolver) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *Resolver) Range(cb func(string, *Builder)) {
	s.l.RLock()
	for target, r := range s.m {
		cb(target, r)
	}
	s.l.RUnlock()
}

func (s *Resolver) GetBy(target string) (b *Builder, ok bool) {
	s.l.RLock()
	b, ok = s.m[target]
	s.l.RUnlock()
	return
}

func (s *Resolver) Get(unique bool, schema, serviceName string) (b *Builder, ok bool) {
	s.l.RLock()
	b, ok = s.m[TargetString(unique, schema, serviceName)]
	s.l.RUnlock()
	return
}

func (s *Resolver) GetAdd(unique bool, schema, etcdAddr, serviceName string) (b *Builder, ok bool) {
	logs.Debugf("%v before.size=%v", TargetString(unique, schema, serviceName), s.Len())
	// s.Range(func(target string, _ *Builder) {
	// 	logs.Debugf("%v", target)
	// })
	b, ok = s.Get(unique, schema, serviceName)
	switch ok {
	case true:
		b.cli.Update(etcdAddr, func(v *Clientv3) {
		})
	default:
		b, ok = s.getAdd(unique, schema, serviceName)
		switch ok {
		case true:
			b.cli.Update(etcdAddr, func(v *Clientv3) {
			})
		}
	}
	logs.Debugf("%v after.size=%v", TargetString(unique, schema, serviceName), s.Len())
	// s.Range(func(target string, _ *Builder) {
	// 	logs.Debugf("%v", target)
	// })
	return
}

func (s *Resolver) getAdd(unique bool, schema, serviceName string) (b *Builder, ok bool) {
	b = newBuilder(schema)
	grpc_resolver.Register(b)
	s.l.Lock()
	s.m[TargetString(unique, schema, serviceName)] = b
	s.l.Unlock()
	ok = true
	return
}

func (s *Resolver) Remove(unique bool, schema, serviceName string) {
	s.l.Lock()
	target := TargetString(unique, schema, serviceName)
	b, ok := s.m[target]
	switch ok {
	case true:
		switch schema == b.Scheme() {
		case false:
			s.l.Unlock()
			goto ERR
		}
		delete(s.m, target)
		grpc_resolver.UnregisterForTesting(schema)
	}
	s.l.Unlock()
ERR:
	logs.Fatalf("error")
}

func (s *Resolver) RemoveBy(schema, target string) {
	s.l.Lock()
	b, ok := s.m[target]
	switch ok {
	case true:
		switch schema == b.Scheme() {
		case false:
			s.l.Unlock()
			goto ERR
		}
		delete(s.m, target)
		grpc_resolver.UnregisterForTesting(schema)
	}
	s.l.Unlock()
ERR:
	logs.Fatalf("error")
}

func TargetString(unique bool, schema, serviceName string) string {
	switch unique {
	case true:
		return GetPrefix4Unique(schema, serviceName)
	default:
		return GetPrefix(schema, serviceName)
	}
}

// ResolveNow
func (s *Resolver) ResolveNow(rn grpc_resolver.ResolveNowOptions) {
}

// Close
func (s *Resolver) Close() {
	s.Range(func(_ string, b *Builder) {
		b.close()
	})
}

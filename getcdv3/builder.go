package getcdv3

import (
	"context"
	"errors"
	"sync"

	"github.com/cwloo/gonet/logs"

	clientv3 "go.etcd.io/etcd/client/v3"

	"strings"
	"time"

	grpc_resolver "google.golang.org/grpc/resolver"
)

// <summary>
// Builder
// <summary>
type Builder interface {
	grpc_resolver.Builder
	Len() (c int)
	Range(cb func(Watcher))
	Get(host string) (w Watcher, ok bool)
	GetAdd(host, target string) (w Watcher, ok bool)
	Remove(host string, cb func(Watcher))
	RangeRemove(cb func(Watcher))
	Close()
}

// <summary>
// Builder_
// <summary>
type Builder_ struct {
	grpc_resolver.Builder
	schema string
	m      map[string]Watcher
	l      *sync.RWMutex
}

func newBuilder(schema string) Builder {
	return &Builder_{
		schema: schema,
		m:      map[string]Watcher{},
		l:      &sync.RWMutex{},
	}
}

func (s *Builder_) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *Builder_) Range(cb func(Watcher)) {
	s.l.RLock()
	for _, w := range s.m {
		cb(w)
	}
	s.l.RUnlock()
}

func (s *Builder_) Get(host string) (w Watcher, ok bool) {
	s.l.RLock()
	w, ok = s.m[host]
	s.l.RUnlock()
	return
}

func (s *Builder_) GetAdd(host, target string) (w Watcher, ok bool) {
	w, ok = s.Get(host)
	switch ok {
	case true:
	default:
		w, ok = s.getAdd(host, target)
	}
	return
}

func (s *Builder_) getAdd(host, target string) (w Watcher, ok bool) {
	s.l.Lock()
	w, ok = s.m[host]
	switch ok {
	case true:
	default:
		w = newWatcher(host, target, s.Remove)
		s.m[host] = w
		ok = true
	}
	s.l.Unlock()
	return
}

func (s *Builder_) remove(host string, cb func(Watcher)) (c int, w Watcher, ok bool) {
	s.l.Lock()
	w, ok = s.m[host]
	switch ok {
	case true:
		cb(w)
		delete(s.m, host)
	}
	c = len(s.m)
	s.l.Unlock()
	return
}

func (s *Builder_) Remove(host string, cb func(Watcher)) {
	_, ok := s.Get(host)
	switch ok {
	case true:
		c, _, ok := s.remove(host, cb)
		switch ok {
		case true:
			switch c {
			case 0:
				s.reset()
			}
		}
	default:
	}
}

func (s *Builder_) RangeRemove(cb func(Watcher)) {
	s.l.Lock()
	for host, w := range s.m {
		cb(w)
		delete(s.m, host)
	}
	s.l.Unlock()
	s.reset()
}

func ParseTarget(target grpc_resolver.Target) (schema, serviceName string, unique bool) {
	schema = target.URL.Scheme
	serviceName = target.URL.Path
	switch len(serviceName) > 0 && serviceName[0] == '/' {
	case true:
		serviceName = strings.Replace(serviceName, "/", "", 1)
	}
	switch len(serviceName) > 0 && serviceName[len(serviceName)-1:][0] == '/' {
	case true:
		serviceName = strings.Replace(serviceName, "/", "", 1)
	}
	// first := strings.Index(serviceName, "/")
	// last := strings.LastIndex(serviceName, "/")
	// switch first <= last {
	// case true:
	// 	serviceName = serviceName[first+1 : last]
	// }
	unique = serviceName[len(serviceName)-1:][0] == '/'
	return
}

// Build
func (s *Builder_) Build(resolver_target grpc_resolver.Target, cc grpc_resolver.ClientConn, _ grpc_resolver.BuildOptions) (grpc_resolver.Resolver, error) {
	schema, serviceName, unique := ParseTarget(resolver_target)
	target := TargetString(unique, schema, serviceName)
	switch s.schema == schema {
	case true:
	default:
		logs.Fatalf("error")
	}
	cli := newClient(false)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	resp, err := cli.GetRelease(ctx, target, clientv3.WithPrefix())
	switch err {
	case nil:
		switch len(resp.Kvs) {
		case 0:
			logs.Errorf("%v", target)
			// s.Remove(target)
		default:
			var addrs []grpc_resolver.Address
			for i := range resp.Kvs {
				// logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
				addrs = append(addrs, grpc_resolver.Address{Addr: string(resp.Kvs[i].Value)})
				msg := &WatcherMsg{
					cc:     cc,
					target: target,
					hosts:  map[string]bool{}}
				msg.hosts[string(resp.Kvs[i].Value)] = true
				watcher, _ := s.GetAdd(string(resp.Kvs[i].Value), target)
				watcher.Update(resp.Header.Revision + 1)
				watcher.Watch(msg)
			}
			cc.UpdateState(grpc_resolver.State{Addresses: addrs})
		}
	default:
		logs.Errorf("%v %v", target, err.Error())
		return newResolver(target), errors.New(logs.SprintErrorf("%v %v", target, err.Error()))
	}
	return newResolver(target), nil
}

// Scheme
func (s *Builder_) Scheme() string {
	return s.schema
}

func (s *Builder_) Close() {
	s.RangeRemove(func(w Watcher) {
		w.Close()
	})
	s.reset()
}

func (s *Builder_) reset() {
	switch len(s.m) {
	case 0:
	default:
		logs.Fatalf("error")
	}
	manager.Remove(s.schema, func(string, Builder) {})
}

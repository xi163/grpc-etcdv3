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
type Builder struct {
	grpc_resolver.Builder
	schema string
	m      map[string]*Watcher
	l      *sync.RWMutex
}

func newBuilder(schema string) *Builder {
	logs.Tracef("%v", schema)
	return &Builder{
		schema: schema,
		m:      map[string]*Watcher{},
		l:      &sync.RWMutex{},
	}
}

func (s *Builder) Len() (c int) {
	s.l.RLock()
	c = len(s.m)
	s.l.RUnlock()
	return
}

func (s *Builder) Range(cb func(string, string, *Watcher)) {
	s.l.RLock()
	for target, w := range s.m {
		cb(s.schema, target, w)
	}
	s.l.RUnlock()
}

func (s *Builder) Get(target string) (w *Watcher, ok bool) {
	// logs.Debugf("%v begin size=%v", target, s.Len())
	s.l.RLock()
	w, ok = s.m[target]
	s.l.RUnlock()
	// logs.Debugf("%v end size=%v", target, s.Len())
	return
}

func (s *Builder) GetAdd(target string) (w *Watcher, ok bool) {
	w, ok = s.Get(target)
	switch ok {
	case true:
	default:
		w, ok = s.getAdd(target)
	}
	return
}

func (s *Builder) getAdd(target string) (w *Watcher, ok bool) {
	// logs.Debugf("%v begin size=%v", target, s.Len())
	s.l.Lock()
	w, ok = s.m[target]
	switch ok {
	case true:
	default:
		w = newWatcher(target, s.Remove)
		s.m[target] = w
		ok = true
	}
	s.l.Unlock()
	// logs.Debugf("%v end size=%v", target, s.Len())
	return
}

func (s *Builder) Remove(target string) {
	c, _, ok := s.remove(target)
	switch ok {
	case true:
		switch c {
		case 0:
			s.reset()
		}
	}
}

func (s *Builder) remove(target string) (c int, w *Watcher, ok bool) {
	s.l.Lock()
	w, ok = s.m[target]
	switch ok {
	case true:
		logs.Errorf("%v begin size=%v", target, len(s.m))
		ctx, _ := context.WithCancel(context.Background())
		// w.cli.Cancel()
		w.cli.Delete(ctx, target)
		w.NotifyClose()
		delete(s.m, target)
		logs.Errorf("%v end size=%v", target, len(s.m))
	}
	c = len(s.m)
	s.l.Unlock()
	return
}

func (s *Builder) RangeRemove(cb func(string, string, *Watcher)) {
	ctx, _ := context.WithCancel(context.Background())
	s.l.Lock()
	for target, w := range s.m {
		logs.Errorf("%v begin size=%v", target, len(s.m))
		cb(s.schema, target, w)
		// w.cli.Cancel()
		w.cli.Delete(ctx, target)
		w.NotifyClose()
		delete(s.m, target)
		logs.Errorf("%v end size=%v", target, len(s.m))
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
func (s *Builder) Build(resolver_target grpc_resolver.Target, cc grpc_resolver.ClientConn, _ grpc_resolver.BuildOptions) (grpc_resolver.Resolver, error) {
	// logs.Errorf("%v", resolver_target.URL)
	schema, serviceName, unique := ParseTarget(resolver_target)
	target := TargetString(unique, schema, serviceName)
	s.schema = schema
	watcher, ok := s.Get(target)
	switch ok {
	case true:
	default:
		logs.Fatalf("error")
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := watcher.cli.Get(ctx, target, clientv3.WithPrefix())
	switch err {
	case nil:
		msg := &WatcherMsg{
			cc:     cc,
			target: target,
			hosts:  map[string]bool{}}
		var addrs []grpc_resolver.Address
		for i := range resp.Kvs {
			// logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			addrs = append(addrs, grpc_resolver.Address{Addr: string(resp.Kvs[i].Value)})
			msg.hosts[string(resp.Kvs[i].Value)] = true
		}
		switch len(resp.Kvs) {
		case 0:
			logs.Errorf("%v", target)
			// s.Remove(target)
		default:
			cc.UpdateState(grpc_resolver.State{Addresses: addrs})
			watcher.revision = resp.Header.Revision + 1
			watcher.Watch(msg)
		}
	default:
		logs.Fatalf("%v %v", target, err.Error())
		return nil, errors.New(logs.SprintErrorf("%v %v", target, err.Error()))
	}
	return watcher.r, nil
}

// Scheme
func (s *Builder) Scheme() string {
	return s.schema
}

func (s *Builder) Close() {
	s.RangeRemove(func(_, _ string, w *Watcher) {
		w.Close()
	})
	s.reset()
}

func (s *Builder) reset() {
	switch len(s.m) {
	case 0:
	default:
		logs.Fatalf("error")
	}
	manager.Remove(s.schema)
}

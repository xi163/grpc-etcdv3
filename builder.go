package getcdv3

import (
	"context"
	"errors"
	"runtime"

	"github.com/cwloo/gonet/core/base/cc"
	"github.com/cwloo/gonet/core/base/mq"
	"github.com/cwloo/gonet/core/base/mq/lq"
	"github.com/cwloo/gonet/logs"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"strings"
	"time"

	grpc_resolver "google.golang.org/grpc/resolver"
)

// <summary>
// ExitStruct
// <summary>
type ExitStruct struct {
	schema string
	target string
}

// <summary>
// Watcher
// <summary>
type Watcher struct {
	target string
	cc     grpc_resolver.ClientConn
	hosts  map[string]bool
}

// <summary>
// Builder
// <summary>
type Builder struct {
	grpc_resolver.Builder
	schema        string
	target        string
	watchRevision int64
	watched       bool
	mq            mq.Queue
	flag          cc.AtomFlag
}

func newBuilder(schema string) *Builder {
	s := &Builder{
		schema: schema,
		flag:   cc.NewAtomFlag(),
		mq:     lq.NewList(1000),
	}
	return s
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
	logs.Errorf("%v", resolver_target)
	schema, serviceName, unique := ParseTarget(resolver_target)
	target := TargetString(unique, schema, serviceName)
	_, ok := resolver.Get(unique, schema, serviceName)
	switch ok {
	case false:
		logs.Fatalf("error")
	}
	s.schema = schema
	s.target = target
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := cli.GetCtx(ctx, target, clientv3.WithPrefix())
	switch err {
	case nil:
		watcher := &Watcher{
			cc:     cc,
			target: target,
			hosts:  map[string]bool{}}
		var addrs []grpc_resolver.Address
		for i := range resp.Kvs {
			logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			addrs = append(addrs, grpc_resolver.Address{Addr: string(resp.Kvs[i].Value)})
			watcher.hosts[string(resp.Kvs[i].Value)] = true
		}
		cc.UpdateState(grpc_resolver.State{Addresses: addrs})
		s.watchRevision = resp.Header.Revision + 1
		s.watch(watcher)
	default:
		return nil, errors.New(logs.SprintErrorf("%v", target))
	}
	return resolver, nil
}

// Scheme
func (s *Builder) Scheme() string {
	return s.schema
}

func (s *Builder) watch(wacher *Watcher) {
	switch len(wacher.hosts) > 0 {
	case true:
		switch !s.watched && s.flag.TestSet() {
		case true:
			go s.watching()
			s.watched = true
			s.flag.Reset()
		}
		s.mq.Push(wacher)
	}
}

func (s *Builder) watching() {
	i, t := 0, 200
EXIT:
	for {
		if i > t {
			i = 0
			runtime.GC()
		}
		i++
		exit, _ := s.mq.Exec(false, s.handler, nil)
		if exit {
			break EXIT
		}
	}
}

func (s *Builder) handler(msg any, args ...any) (exit bool) {
	switch msg := msg.(type) {
	case *ExitStruct:
		exit = true
		s.watched = false
		resolver.RemoveBy(msg.schema, msg.target)
	case *Watcher:
		s.Watch_handler(msg)
	}
	return
}

func (s *Builder) Watch_handler(msg *Watcher) {
	logs.Debugf("%+v", msg)
	watchChan := cli.WatchCtx(context.Background(), msg.target, clientv3.WithPrefix(), clientv3.WithPrefix())
	for resp := range watchChan {
		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				_, ok := msg.hosts[string(ev.Kv.Value)]
				switch ok {
				case true:
					logs.Errorf("<PUT> builder:%+v watch:%v", msg.hosts, string(ev.Kv.Value))
				default:
					logs.Debugf("<PUT> builder:%+v watch:%v", msg.hosts, string(ev.Kv.Value))
					hosts := []grpc_resolver.Address{}
					for host := range msg.hosts {
						hosts = append(hosts, grpc_resolver.Address{Addr: host})
					}
					msg.cc.UpdateState(grpc_resolver.State{Addresses: hosts})
					msg.hosts[string(ev.Kv.Value)] = true
				}
			case mvccpb.DELETE:
				i := strings.LastIndexAny(string(ev.Kv.Key), "/")
				switch i >= 0 {
				case true:
					host := string(ev.Kv.Key)[i+1:]
					logs.Debugf("<DELETE> %v => %v", string(ev.Kv.Key), host)
					_, ok := msg.hosts[host]
					switch ok {
					case true:
						rpcConns.RemoveConnByHost(host)
						delete(msg.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range msg.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						msg.cc.UpdateState(grpc_resolver.State{Addresses: hosts})
					default:
						logs.Errorf("<DELETE> %v => %v", string(ev.Kv.Key), host)
					}
				default:
					continue
				}
			}
		}
	}
}

func (s *Builder) close() {
	switch s.watched {
	case true:
		s.mq.Push(&ExitStruct{schema: s.schema, target: s.target})
	default:
		resolver.RemoveBy(s.schema, s.target)
	}
}

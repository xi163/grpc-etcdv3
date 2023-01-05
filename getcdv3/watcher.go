package getcdv3

import (
	"context"
	"runtime"
	"strings"
	"sync"

	"github.com/cwloo/gonet/core/base/cc"
	"github.com/cwloo/gonet/core/base/mq"
	"github.com/cwloo/gonet/core/base/mq/lq"
	"github.com/cwloo/gonet/logs"
	"github.com/cwloo/grpc-etcdv3/getcdv3/gRPCs"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	grpc_resolver "google.golang.org/grpc/resolver"
)

// <summary>
// Watcher
// <summary>
type Watcher interface {
	Cli() Client
	Target() string
	Update(revision int64)
	Watch(msg *WatcherMsg)
	Close()
	NotifyClose()
	Put()
}

// <summary>
// ExitStruct
// <summary>
type ExitStruct struct {
	target string
}

// <summary>
// WatcherMsg
// <summary>
type WatcherMsg struct {
	target string
	cc     grpc_resolver.ClientConn
	hosts  map[string]bool
}

// <summary>
// watcher
// <summary>
type watcher struct {
	// we can't inherit from grpc_resolver.Resolver, if do so,
	// grpc_resolver.Resolver.Close() will destroy watcher, then it will be unpredictable !
	watched  bool
	cli      Client
	revision int64
	host     string
	target   string
	mq       mq.Queue
	l        *sync.Mutex
	cond     *sync.Cond
	stopping cc.Singal
	flag     cc.AtomFlag
	cancel   cc.AtomFlag
	hosts    map[string]bool
	cc       map[grpc_resolver.ClientConn]bool
	cb       func(string, func(Watcher))
}

func newWatcher(host, target string, cb func(string, func(Watcher))) Watcher {
	s := &watcher{
		cli:      newClient(false),
		cb:       cb,
		host:     host,
		target:   target,
		l:        &sync.Mutex{},
		flag:     cc.NewAtomFlag(),
		cancel:   cc.NewAtomFlag(),
		stopping: cc.NewSingal(),
		hosts:    map[string]bool{},
		cc:       map[grpc_resolver.ClientConn]bool{},
	}
	s.cond = sync.NewCond(s.l)
	s.sched()
	return s
}

func (s *watcher) Cli() Client {
	return s.cli
}

func (s *watcher) Target() string {
	return s.target
}

func (s *watcher) cleanup() {
	ctx, _ := context.WithCancel(context.Background())
	// s.Cli().Cancel()
	s.Cli().Delete(ctx, s.target)
}

func (s *watcher) reset() {
	s.cb(s.host, func(_ Watcher) {})
	s.cleanup()
	s.mq = nil
}

func (s *watcher) Put() {
	s.reset()
}

func (s *watcher) Close() {
	switch s.watched {
	case false:
		s.Put()
	default:
		s.stop()
		s.wait_stop()
	}
}

func (s *watcher) NotifyClose() {
	switch s.watched {
	case false:
		s.Put()
	default:
		s.stop()
	}
}

func (s *watcher) onQuit() {
	s.Put()
}

func (s *watcher) Update(revision int64) {
	s.revision = revision
}

func (s *watcher) Watch(msg *WatcherMsg) {
	switch len(msg.hosts) > 0 {
	case true:
		s.sched()
		s.mq.Push(msg)
	}
}

func (s *watcher) run() {
	s.l.Lock()
	s.watched = true
	s.cond.Signal()
	s.l.Unlock()

	s.running()
	s.onQuit()

	s.l.Lock()
	s.watched = false
	s.cond.Signal()
	s.l.Unlock()
}

func (s *watcher) running() (exit bool) {
	i, t := 0, 200
LOOP:
	for {
		if i > t {
			i = 0
			runtime.GC()
		}
		i++
		exit := s.handler()
		switch exit {
		case true:
			break LOOP
		}
	}
	return
}

func (s *watcher) handler() (exit bool) {
	exit = s.watching()
	switch exit {
	case true:
		logs.Tracef("--------------------- ****** exit %v", s.target)
	}
	return
}

func (s *watcher) watching() (exit bool) {
	exit = s.watch_handler(s.cli.WatchRelease(
		context.Background(),
		s.target,
		clientv3.WithPrefix(),
		clientv3.WithPrefix()))
	return
}

func (s *watcher) pick() (exit bool) {
	v := s.mq.Pick()
	for _, msg := range v {
		switch msg := msg.(type) {
		case *ExitStruct:
			goto EXIT
		case *WatcherMsg:
			switch s.target == msg.target {
			case true:
			default:
				logs.Fatalf("error")
			}
			for host := range msg.hosts {
				s.hosts[host] = true
			}
			s.cc[msg.cc] = true
		}
	}
EXIT:
	exit = true
	return
}

func (s *watcher) watch_handler(watchChan clientv3.WatchChan) (exit bool) {
	select { // block here
	case resp := <-watchChan:
		EXIT := s.pick()
		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				_, ok := s.hosts[string(ev.Kv.Value)]
				switch ok {
				case true:
					s.cancel.TestSet()
					logs.Errorf("<PUT> %v builder:%+v watch:%v", s.target, s.hosts, string(ev.Kv.Value))
				default:
					s.cancel.TestSet()
					logs.Debugf("<PUT> %v builder:%+v watch:%v", s.target, s.hosts, string(ev.Kv.Value))
					hosts := []grpc_resolver.Address{}
					for host := range s.hosts {
						hosts = append(hosts, grpc_resolver.Address{Addr: host})
					}
					for cc := range s.cc {
						cc.UpdateState(grpc_resolver.State{Addresses: hosts})
					}
					s.hosts[string(ev.Kv.Value)] = true
				}
			case mvccpb.DELETE:
				i := strings.LastIndexAny(string(ev.Kv.Key), "/")
				switch i >= 0 {
				case true:
					host := string(ev.Kv.Key)[i+1:]
					_, ok := s.hosts[host]
					switch ok {
					case true:
						s.cancel.TestReset()
						logs.Debugf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
						clients, ok := gRPCs.Conns().Get(s.target)
						switch ok {
						case true:
							clients.Remove(host)
						default:
							unique, schema, serviceName, _ := TargetToHost(s.target)
							target := TargetString(unique, schema, serviceName)
							clients, ok := gRPCs.Conns().Get(target)
							switch ok {
							case true:
								clients.Remove(host)
							default:
								logs.Fatalf("error")
							}
						}
						delete(s.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range s.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						for cc := range s.cc {
							cc.UpdateState(grpc_resolver.State{Addresses: hosts})
						}
						s.cb(s.host, func(_ Watcher) {})
						s.stop()
					default:
						s.cancel.TestReset()
						logs.Errorf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
						clients, ok := gRPCs.Conns().Get(s.target)
						switch ok {
						case true:
							clients.Remove(host)
						default:
							unique, schema, serviceName, _ := TargetToHost(s.target)
							target := TargetString(unique, schema, serviceName)
							clients, ok := gRPCs.Conns().Get(target)
							switch ok {
							case true:
								clients.Remove(host)
							default:
								logs.Fatalf("error")
							}
						}
						delete(s.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range s.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						for cc := range s.cc {
							cc.UpdateState(grpc_resolver.State{Addresses: hosts})
						}
						s.cb(s.host, func(_ Watcher) {})
						s.stop()
					}
				default:
					host := string(ev.Kv.Key)
					logs.Errorf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
				}
			default:
			}
		}
		switch EXIT {
		case true:
			s.stop()
		}
	case <-s.stopping.Read():
		s.stopped()
		goto EXIT
		// default:
	}
	return
EXIT:
	s.cleanup()
	exit = true
	return
}

func (s *watcher) sched() {
	switch s.watched {
	case true:
	default:
		switch s.flag.TestSet() {
		case true:
			logs.Tracef("--------------------- ****** %v", s.target)
			s.mq = lq.NewList(1000)
			go s.run()
			s.wait()
			s.flag.Reset()
		default:
		}
	}
}

func (s *watcher) wait() {
	s.l.Lock()
	for !s.watched {
		s.cond.Wait()
	}
	s.l.Unlock()
}

func (s *watcher) stop() {
	switch s.cancel.IsSet() {
	case true:
	default:
		s.stopping.Signal()
	}
}

func (s *watcher) stopped() {
	// logs.Debugf("%v", s.target)
}

func (s *watcher) wait_stop() {
	s.l.Lock()
	for s.watched {
		s.cond.Wait()
	}
	s.l.Unlock()
}

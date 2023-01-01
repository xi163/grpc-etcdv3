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
	Resolver() *Resolver
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
// Watcher_
// <summary>
type Watcher_ struct {
	// we can't inherit from grpc_resolver.Resolver, if do so,
	// grpc_resolver.Resolver.Close() will destroy Watcher_, then it will be unpredictable !
	r        *Resolver
	cli      Client
	revision int64
	target   string
	cb       func(string)
	watched  bool
	mq       mq.Queue
	l        *sync.Mutex
	cond     *sync.Cond
	stopping cc.Singal
	flag     cc.AtomFlag
	hosts    map[string]bool
	cc       map[grpc_resolver.ClientConn]bool
}

func newWatcher(target string, cb func(string)) Watcher {
	logs.Tracef("%v", target)
	s := &Watcher_{
		r:        newResolver(target),
		cli:      newClient(),
		cb:       cb,
		target:   target,
		l:        &sync.Mutex{},
		flag:     cc.NewAtomFlag(),
		stopping: cc.NewSingal(),
		hosts:    map[string]bool{},
		cc:       map[grpc_resolver.ClientConn]bool{},
	}
	s.cond = sync.NewCond(s.l)
	s.sched()
	return s
}

func (s *Watcher_) Target() string {
	return s.target
}

func (s *Watcher_) Cli() Client {
	return s.cli
}

func (s *Watcher_) Resolver() *Resolver {
	return s.r
}

func (s *Watcher_) reset() {
	s.cb(s.target)
	s.mq = nil
}

func (s *Watcher_) Put() {
	s.reset()
}

func (s *Watcher_) Close() {
	logs.Errorf("%v", s.target)
	switch s.watched {
	case false:
		s.Put()
	default:
		s.stop()
		s.wait_stop()
	}
}

func (s *Watcher_) NotifyClose() {
	logs.Errorf("%v", s.target)
	switch s.watched {
	case false:
		s.Put()
	default:
		s.stop()
	}
}

func (s *Watcher_) onQuit() {
	logs.Errorf("%v", s.target)
	s.Put()
}

func (s *Watcher_) Update(revision int64) {
	s.revision = revision
}

func (s *Watcher_) Watch(msg *WatcherMsg) {
	switch len(msg.hosts) > 0 {
	case true:
		s.sched()
		s.mq.Push(msg)
	}
}

func (s *Watcher_) run() {
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

func (s *Watcher_) running() (exit bool) {
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

func (s *Watcher_) handler() (exit bool) {
	exit = s.watching()
	switch exit {
	case true:
		logs.Tracef("--------------------- ****** exit %v", s.target)
	}
	return
}

func (s *Watcher_) watching() (exit bool) {
	exit = s.watch_handler(s.cli.WatchRelease(
		context.Background(),
		s.target,
		clientv3.WithPrefix(),
		clientv3.WithPrefix()))
	logs.Warnf("end %v", s.target)
	return
}

func (s *Watcher_) pick() (exit bool) {
	v := s.mq.Pick()
	for _, msg := range v {
		switch msg := msg.(type) {
		case *ExitStruct:
			logs.Errorf("%v", msg.target)
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

func (s *Watcher_) watch_handler(watchChan clientv3.WatchChan) (exit bool) {
	select { // block here
	case resp := <-watchChan:
		logs.Warnf("begin %v", s.target)
		EXIT := s.pick()
		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				_, ok := s.hosts[string(ev.Kv.Value)]
				switch ok {
				case true:
					logs.Errorf("<PUT> %v builder:%+v watch:%v", s.target, s.hosts, string(ev.Kv.Value))
				default:
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
						logs.Debugf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
						rpcConns.RemoveConn(host)
						delete(s.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range s.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						for cc := range s.cc {
							cc.UpdateState(grpc_resolver.State{Addresses: hosts})
						}
						s.cb(s.target)
					default:
						logs.Errorf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
						rpcConns.RemoveConn(host)
						delete(s.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range s.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						for cc := range s.cc {
							cc.UpdateState(grpc_resolver.State{Addresses: hosts})
						}
						s.cb(s.target)
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
			s.stopping.Signal()
		}
	case <-s.stopping.Read():
		logs.Debugf("stopping %v", s.target)
		goto EXIT
		// default:
	}
	return
EXIT:
	exit = true
	return
}

func (s *Watcher_) sched() {
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

func (s *Watcher_) wait() {
	s.l.Lock()
	for !s.watched {
		s.cond.Wait()
	}
	s.l.Unlock()
}

func (s *Watcher_) stop() {
	s.stopping.Signal()
}

func (s *Watcher_) wait_stop() {
	s.l.Lock()
	for s.watched {
		s.cond.Wait()
	}
	s.l.Unlock()
}
